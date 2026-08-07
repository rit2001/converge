import Fastify, { type FastifyInstance } from "fastify";
import cors from "@fastify/cors";
import rateLimit from "@fastify/rate-limit";
import { Server, type DefaultEventsMap } from "socket.io";
import { z } from "zod";
import { BoardRepository, RepositoryError, type DatabasePool } from "@converge/database";
import {
  MAX_SYNC_BATCH_SIZE,
  createBoardRequestSchema,
  durableCommandSchema,
  joinBoardAckSchema,
  joinBoardRequestSchema,
  operationRangeQuerySchema,
  operationRangeResponseSchema,
  type ClientToServerEvents,
  type OperationAck,
  type ProtocolError,
  type ServerToClientEvents,
} from "@converge/protocol";
import { AuthenticationError, type AuthAdapter, type AuthenticatedPrincipal } from "./auth.js";
import type { Environment } from "./env.js";

function errorStatus(code: RepositoryError["code"]): number {
  if (code === "BOARD_NOT_FOUND") return 404;
  if (code === "FORBIDDEN") return 403;
  return 409;
}

function failedAck(error: unknown): ProtocolError {
  if (error instanceof AuthenticationError)
    return { ok: false, code: error.code, message: error.message, retryable: false };
  if (error instanceof RepositoryError)
    return {
      ok: false,
      code: error.code,
      message: error.message,
      retryable: error.code === "RESYNC_REQUIRED",
    };
  if (error instanceof z.ZodError)
    return {
      ok: false,
      code: "INVALID_COMMAND",
      message: "Command validation failed",
      retryable: false,
    };
  return {
    ok: false,
    code: "INTERNAL_ERROR",
    message: "Operation could not be committed",
    retryable: true,
  };
}

function socketAuthenticationError(error: AuthenticationError): Error {
  return Object.assign(new Error(error.message), {
    data: { code: error.code, message: error.message, retryable: false },
  });
}

export interface AppContext {
  app: FastifyInstance;
  io: Server<ClientToServerEvents, ServerToClientEvents, DefaultEventsMap, AuthenticatedSocketData>;
}

interface AuthenticatedSocketData {
  principal: AuthenticatedPrincipal;
}

export interface SynchronizationHooks {
  afterRoomJoin?: (context: { boardId: string; userId: string; socketId: string }) => Promise<void>;
}

export interface BuildAppOptions {
  synchronizationBatchSize?: number;
  synchronizationHooks?: SynchronizationHooks;
}

export async function buildApp(
  environment: Environment,
  pool: DatabasePool,
  auth: AuthAdapter,
  options: BuildAppOptions = {},
): Promise<AppContext> {
  const app = Fastify({ logger: { level: environment.LOG_LEVEL }, bodyLimit: 64 * 1024 });
  await app.register(cors, { origin: environment.WEB_ORIGIN, credentials: true });
  await app.register(rateLimit, { max: 120, timeWindow: "1 minute" });
  const repository = new BoardRepository(pool);
  const synchronizationBatchSize = z
    .number()
    .int()
    .positive()
    .max(MAX_SYNC_BATCH_SIZE)
    .parse(options.synchronizationBatchSize ?? MAX_SYNC_BATCH_SIZE);

  app.setErrorHandler((error, _request, reply) => {
    if (error instanceof AuthenticationError)
      return reply
        .code(error.code === "AUTHENTICATION_REQUIRED" ? 401 : 400)
        .send({ code: error.code, message: error.message, retryable: false });
    if (error instanceof z.ZodError)
      return reply.code(400).send({
        code: "INVALID_COMMAND",
        message: "Request validation failed",
        retryable: false,
      });
    return reply.send(error);
  });

  const authenticateHttp = async (request: Parameters<AuthAdapter["authenticateHttp"]>[0]) => {
    const principal = await auth.authenticateHttp(request);
    if (!principal)
      throw new AuthenticationError("AUTHENTICATION_REQUIRED", "Authentication required");
    return principal;
  };

  app.get("/health", () => ({ ok: true }));
  app.post("/v1/boards", async (request, reply) => {
    const user = await authenticateHttp(request);
    const body = createBoardRequestSchema.parse(request.body);
    return reply.code(201).send(await repository.createBoard(user.id, body.name));
  });
  app.get<{ Params: { boardId: string } }>("/v1/boards/:boardId", async (request, reply) => {
    const user = await authenticateHttp(request);
    try {
      return await repository.getBoard(z.string().uuid().parse(request.params.boardId), user.id);
    } catch (error) {
      if (error instanceof RepositoryError)
        return reply
          .code(errorStatus(error.code))
          .send({ code: error.code, message: error.message });
      throw error;
    }
  });
  app.get<{ Params: { boardId: string }; Querystring: { after: string; watermark: string } }>(
    "/v1/boards/:boardId/operations",
    async (request, reply) => {
      const user = await authenticateHttp(request);
      const boardId = z.string().uuid().parse(request.params.boardId);
      const range = operationRangeQuerySchema.parse(request.query);
      try {
        return {
          ...operationRangeResponseSchema.parse(
            await repository.getOperationBatch(
              boardId,
              user.id,
              range.after,
              range.watermark,
              synchronizationBatchSize,
            ),
          ),
        };
      } catch (error) {
        if (error instanceof RepositoryError)
          return reply.code(errorStatus(error.code)).send({
            code: error.code,
            message: error.message,
            retryable: error.code === "RESYNC_REQUIRED",
          });
        throw error;
      }
    },
  );

  const io = new Server<
    ClientToServerEvents,
    ServerToClientEvents,
    DefaultEventsMap,
    AuthenticatedSocketData
  >(app.server, {
    cors: { origin: environment.WEB_ORIGIN, credentials: true },
    maxHttpBufferSize: 64 * 1024,
  });
  io.use((socket, next) => {
    void (async () => {
      try {
        const principal = await auth.authenticateSocket(socket);
        if (!principal)
          throw new AuthenticationError("AUTHENTICATION_REQUIRED", "Authentication required");
        socket.data.principal = principal;
        next();
      } catch (error) {
        if (error instanceof AuthenticationError) next(socketAuthenticationError(error));
        else next(new Error("Authentication failed"));
      }
    })();
  });
  io.on("connection", (socket) => {
    const user = socket.data.principal;
    let windowStarted = Date.now();
    let commandsInWindow = 0;
    socket.on("board:join", async (raw, acknowledge) => {
      try {
        const request = joinBoardRequestSchema.parse(raw);
        if (!(await repository.roleFor(request.boardId, user.id)))
          throw new RepositoryError("FORBIDDEN", "Board membership required");
        await socket.join(`board:${request.boardId}`);
        await options.synchronizationHooks?.afterRoomJoin?.({
          boardId: request.boardId,
          userId: user.id,
          socketId: socket.id,
        });
        const joinWatermark = await repository.getBoardSequence(request.boardId, user.id);
        if (request.lastAppliedSeq > joinWatermark)
          throw new RepositoryError(
            "RESYNC_REQUIRED",
            "Client sequence exceeds authoritative board head",
          );
        acknowledge(
          joinBoardAckSchema.parse({
            ok: true,
            boardId: request.boardId,
            joinWatermark,
          }),
        );
      } catch (error) {
        acknowledge(joinBoardAckSchema.parse(failedAck(error)));
      }
    });
    socket.on("operation:submit", async (raw, acknowledge) => {
      if (Date.now() - windowStarted >= 10_000) {
        windowStarted = Date.now();
        commandsInWindow = 0;
      }
      commandsInWindow += 1;
      if (commandsInWindow > 100) {
        acknowledge({
          ok: false,
          code: "RATE_LIMITED",
          message: "Durable command rate exceeded",
          retryable: true,
        });
        return;
      }
      try {
        if (JSON.stringify(raw).length > 64 * 1024) {
          acknowledge({
            ok: false,
            code: "PAYLOAD_TOO_LARGE",
            message: "Command exceeds 64 KiB",
            retryable: false,
          });
          return;
        }
        const command = durableCommandSchema.parse(raw);
        const committed = await repository.commitOperation(user.id, command);
        const ack: OperationAck = {
          ok: true,
          duplicate: committed.duplicate,
          operation: committed.operation,
        };
        acknowledge(ack);
        if (!committed.duplicate)
          io.to(`board:${command.boardId}`).emit("operation:committed", committed.operation);
      } catch (error) {
        app.log.warn({ error, userId: user.id }, "operation rejected");
        acknowledge(failedAck(error));
      }
    });
  });
  return { app, io };
}
