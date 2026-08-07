import Fastify, { type FastifyInstance } from "fastify";
import cors from "@fastify/cors";
import rateLimit from "@fastify/rate-limit";
import { Server } from "socket.io";
import { z } from "zod";
import { BoardRepository, RepositoryError, type DatabasePool } from "@converge/database";
import {
  createBoardRequestSchema,
  durableCommandSchema,
  joinBoardRequestSchema,
  operationRangeQuerySchema,
  type ClientToServerEvents,
  type OperationAck,
  type ServerToClientEvents,
} from "@converge/protocol";
import type { AuthenticationAdapter } from "./auth.js";
import type { Environment } from "./env.js";

function errorStatus(code: RepositoryError["code"]): number {
  if (code === "BOARD_NOT_FOUND") return 404;
  if (code === "FORBIDDEN") return 403;
  return 409;
}

function failedAck(error: unknown): OperationAck {
  if (error instanceof RepositoryError)
    return { ok: false, code: error.code, message: error.message, retryable: false };
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

export interface AppContext {
  app: FastifyInstance;
  io: Server<ClientToServerEvents, ServerToClientEvents>;
}

export async function buildApp(
  environment: Environment,
  pool: DatabasePool,
  auth: AuthenticationAdapter,
): Promise<AppContext> {
  const app = Fastify({ logger: { level: environment.LOG_LEVEL }, bodyLimit: 64 * 1024 });
  await app.register(cors, { origin: environment.WEB_ORIGIN, credentials: true });
  await app.register(rateLimit, { max: 120, timeWindow: "1 minute" });
  const repository = new BoardRepository(pool);

  app.get("/health", () => ({ ok: true }));
  app.post("/v1/boards", async (request, reply) => {
    const user = await auth.authenticateHttp(request);
    if (!user) return reply.code(401).send({ code: "AUTHENTICATION_REQUIRED" });
    const body = createBoardRequestSchema.parse(request.body);
    return reply.code(201).send(await repository.createBoard(user.id, body.name));
  });
  app.get<{ Params: { boardId: string } }>("/v1/boards/:boardId", async (request, reply) => {
    const user = await auth.authenticateHttp(request);
    if (!user) return reply.code(401).send({ code: "AUTHENTICATION_REQUIRED" });
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
  app.get<{ Params: { boardId: string }; Querystring: { from: string; to: string } }>(
    "/v1/boards/:boardId/operations",
    async (request, reply) => {
      const user = await auth.authenticateHttp(request);
      if (!user) return reply.code(401).send({ code: "AUTHENTICATION_REQUIRED" });
      const boardId = z.string().uuid().parse(request.params.boardId);
      const range = operationRangeQuerySchema.parse(request.query);
      try {
        return {
          operations: await repository.getOperations(boardId, user.id, range.from, range.to),
        };
      } catch (error) {
        if (error instanceof RepositoryError)
          return reply
            .code(errorStatus(error.code))
            .send({ code: error.code, message: error.message });
        throw error;
      }
    },
  );

  const io = new Server<ClientToServerEvents, ServerToClientEvents>(app.server, {
    cors: { origin: environment.WEB_ORIGIN, credentials: true },
    maxHttpBufferSize: 64 * 1024,
  });
  io.on("connection", async (socket) => {
    const user = await auth.authenticateSocket(socket);
    if (!user) {
      socket.disconnect(true);
      return;
    }
    let windowStarted = Date.now();
    let commandsInWindow = 0;
    socket.on("board:join", async (raw, acknowledge) => {
      try {
        const request = joinBoardRequestSchema.parse(raw);
        if (!(await repository.roleFor(request.boardId, user.id)))
          throw new RepositoryError("FORBIDDEN", "Board membership required");
        await socket.join(`board:${request.boardId}`);
        acknowledge({ ok: true });
      } catch (error) {
        acknowledge(failedAck(error));
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
