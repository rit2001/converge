import type { Writable } from "node:stream";
import Fastify, { type FastifyInstance } from "fastify";
import cors from "@fastify/cors";
import rateLimit from "@fastify/rate-limit";
import { Server, type DefaultEventsMap } from "socket.io";
import { z } from "zod";
import {
  BoardRepository,
  RepositoryError,
  type BoardRepositoryHooks,
  type DatabasePool,
} from "@converge/database";
import {
  MAX_SYNC_BATCH_SIZE,
  boardAccessRevokedEventSchema,
  createBoardRequestSchema,
  durableCommandSchema,
  httpInternalErrorResponseSchema,
  joinBoardAckSchema,
  joinBoardRequestSchema,
  operationAckSchema,
  operationRangeQuerySchema,
  operationRangeResponseSchema,
  protocolErrorSchema,
  removeBoardMemberParamsSchema,
  removeBoardMemberRequestSchema,
  removeBoardMemberResponseSchema,
  type ClientToServerEvents,
  type CommittedOperation,
  type OperationAck,
  type ProtocolError,
  type ServerToClientEvents,
} from "@converge/protocol";
import { AuthenticationError, type AuthAdapter, type AuthenticatedPrincipal } from "./auth.js";
import { BoardDeliveryCoordinator } from "./board-delivery-coordinator.js";
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

function fastifyClientError(
  error: unknown,
): { status: 400 | 413 | 415; response: ProtocolError } | null {
  if (error instanceof Fastify.errorCodes.FST_ERR_CTP_BODY_TOO_LARGE)
    return {
      status: 413,
      response: {
        ok: false,
        code: "PAYLOAD_TOO_LARGE",
        message: "Request body exceeds the maximum allowed size",
        retryable: false,
      },
    };
  if (error instanceof Fastify.errorCodes.FST_ERR_CTP_INVALID_MEDIA_TYPE)
    return {
      status: 415,
      response: {
        ok: false,
        code: "INVALID_COMMAND",
        message: "Request content type is not supported",
        retryable: false,
      },
    };
  if (
    error instanceof Fastify.errorCodes.FST_ERR_CTP_INVALID_CONTENT_LENGTH ||
    error instanceof Fastify.errorCodes.FST_ERR_CTP_EMPTY_JSON_BODY ||
    error instanceof Fastify.errorCodes.FST_ERR_CTP_INVALID_JSON_BODY
  )
    return {
      status: 400,
      response: {
        ok: false,
        code: "INVALID_COMMAND",
        message: "Request body is invalid",
        retryable: false,
      },
    };
  return null;
}

function socketAuthenticationError(error: AuthenticationError): Error {
  return Object.assign(new Error(error.message), {
    data: { code: error.code, message: error.message, retryable: false },
  });
}

export interface AppContext {
  app: FastifyInstance;
  io: AppIo;
}

interface AuthenticatedSocketData {
  principal: AuthenticatedPrincipal;
}

type AppIo = Server<
  ClientToServerEvents,
  ServerToClientEvents,
  DefaultEventsMap,
  AuthenticatedSocketData
>;

type OperationDeliveryStage = "publish" | "acknowledge";

function reportWithoutThrow(
  reportFailure: (stage: OperationDeliveryStage, error: unknown) => void,
  stage: OperationDeliveryStage,
  error: unknown,
): void {
  try {
    reportFailure(stage, error);
  } catch {
    // Delivery error reporting must not destabilize the socket handler.
  }
}

function acknowledgeWithoutThrow(
  acknowledge: (ack: OperationAck) => void,
  ack: OperationAck,
  reportFailure: (stage: OperationDeliveryStage, error: unknown) => void,
): void {
  try {
    acknowledge(operationAckSchema.parse(ack));
  } catch (error) {
    reportWithoutThrow(reportFailure, "acknowledge", error);
  }
}

function publishWithoutThrow(
  publish: () => void,
  reportFailure: (stage: OperationDeliveryStage, error: unknown) => void,
): void {
  try {
    publish();
  } catch (error) {
    reportWithoutThrow(reportFailure, "publish", error);
  }
}

export function deliverCommittedOperation(input: {
  operation: CommittedOperation;
  duplicate: boolean;
  publish: (operation: CommittedOperation) => void;
  acknowledge: (ack: OperationAck) => void;
  reportFailure: (stage: OperationDeliveryStage, error: unknown) => void;
}): void {
  publishWithoutThrow(() => input.publish(input.operation), input.reportFailure);
  acknowledgeWithoutThrow(
    input.acknowledge,
    { ok: true, duplicate: input.duplicate, operation: input.operation },
    input.reportFailure,
  );
}

export interface SynchronizationHooks {
  afterRoomJoin?: (context: { boardId: string; userId: string; socketId: string }) => Promise<void>;
}

export interface DeliveryHooks {
  afterOperationCommit?: (context: {
    boardId: string;
    operation: CommittedOperation;
  }) => Promise<void>;
  afterMembershipCommit?: (context: { boardId: string; revokedUserId: string }) => Promise<void>;
}

export interface BuildAppOptions {
  synchronizationBatchSize?: number;
  synchronizationHooks?: SynchronizationHooks;
  deliveryCoordinator?: BoardDeliveryCoordinator;
  deliveryHooks?: DeliveryHooks;
  repositoryHooks?: BoardRepositoryHooks;
  loggerStream?: Writable;
}

export async function buildApp(
  environment: Environment,
  pool: DatabasePool,
  auth: AuthAdapter,
  options: BuildAppOptions = {},
): Promise<AppContext> {
  const app = Fastify({
    logger:
      options.loggerStream === undefined
        ? { level: environment.LOG_LEVEL }
        : { level: environment.LOG_LEVEL, stream: options.loggerStream },
    bodyLimit: 64 * 1024,
  });
  await app.register(cors, { origin: environment.WEB_ORIGIN, credentials: true });
  await app.register(rateLimit, { max: 120, timeWindow: "1 minute" });
  const repository = new BoardRepository(pool, options.repositoryHooks);
  const deliveryCoordinator = options.deliveryCoordinator ?? new BoardDeliveryCoordinator();
  const synchronizationBatchSize = z
    .number()
    .int()
    .positive()
    .max(MAX_SYNC_BATCH_SIZE)
    .parse(options.synchronizationBatchSize ?? MAX_SYNC_BATCH_SIZE);

  app.setErrorHandler((error, request, reply) => {
    if (reply.sent) return;
    if (error instanceof AuthenticationError)
      return reply
        .code(error.code === "AUTHENTICATION_REQUIRED" ? 401 : 400)
        .send(protocolErrorSchema.parse(failedAck(error)));
    if (error instanceof RepositoryError)
      return reply.code(errorStatus(error.code)).send(failedAck(error));
    if (error instanceof z.ZodError)
      return reply.code(400).send({
        code: "INVALID_COMMAND",
        message: "Request validation failed",
        retryable: false,
      });
    const clientError = fastifyClientError(error);
    if (clientError)
      return reply.code(clientError.status).send(protocolErrorSchema.parse(clientError.response));
    if (error.statusCode === 429)
      return reply.code(429).send({
        ok: false,
        code: "RATE_LIMITED",
        message: "Request rate exceeded",
        retryable: true,
      });
    const requestId = String(request.id).slice(0, 128) || "unknown";
    request.log.error({ err: error, requestId }, "unexpected HTTP request failure");
    return reply.code(500).send(
      httpInternalErrorResponseSchema.parse({
        ok: false,
        code: "INTERNAL_ERROR",
        message: "An internal server error occurred.",
        retryable: true,
        requestId,
      }),
    );
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
          .send(protocolErrorSchema.parse(failedAck(error)));
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
          return reply
            .code(errorStatus(error.code))
            .send(protocolErrorSchema.parse(failedAck(error)));
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

  const boardRoom = (boardId: string): string => `board:${boardId}`;
  const evictBoardMember = async (boardId: string, revokedUserId: string): Promise<void> => {
    const event = boardAccessRevokedEventSchema.parse({
      schemaVersion: 1,
      boardId,
      code: "ACCESS_REVOKED",
      message: "Board access was revoked",
    });
    const room = boardRoom(boardId);
    const targets = [...io.sockets.sockets.values()].filter(
      (candidate) => candidate.data.principal.id === revokedUserId && candidate.rooms.has(room),
    );
    await Promise.all(
      targets.map(async (target) => {
        target.emit("board:access-revoked", event);
        await target.leave(room);
      }),
    );
  };

  app.delete<{
    Params: { boardId: string; userId: string };
    Querystring: Record<string, unknown>;
  }>("/v1/boards/:boardId/members/:userId", async (request, reply) => {
    const user = await authenticateHttp(request);
    const params = removeBoardMemberParamsSchema.parse(request.params);
    removeBoardMemberRequestSchema.parse(request.query);
    removeBoardMemberRequestSchema.parse(request.body ?? {});
    try {
      const result = await deliveryCoordinator.run(params.boardId, async () => {
        const removed = await repository.removeBoardMember(user.id, params.boardId, params.userId);
        try {
          await options.deliveryHooks?.afterMembershipCommit?.({
            boardId: params.boardId,
            revokedUserId: params.userId,
          });
        } catch (error) {
          app.log.warn({ error, boardId: params.boardId }, "membership delivery hook failed");
        }
        await evictBoardMember(params.boardId, params.userId);
        return removed;
      });
      return reply.send(
        removeBoardMemberResponseSchema.parse({
          ok: true,
          boardId: params.boardId,
          userId: params.userId,
          removed: result.removed,
          eventId: result.event?.eventId ?? null,
        }),
      );
    } catch (error) {
      if (error instanceof RepositoryError)
        return reply.code(errorStatus(error.code)).send(failedAck(error));
      throw error;
    }
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
      if (typeof acknowledge !== "function") {
        app.log.warn({ socketId: socket.id }, "board join requires an acknowledgement callback");
        return;
      }
      try {
        const request = joinBoardRequestSchema.parse(raw);
        await deliveryCoordinator.run(request.boardId, async () => {
          if (!(await repository.roleFor(request.boardId, user.id)))
            throw new RepositoryError("FORBIDDEN", "Board membership required");
          await socket.join(boardRoom(request.boardId));
        });
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
      const reportDeliveryFailure = (stage: OperationDeliveryStage, error: unknown): void =>
        app.log.warn(
          { error, stage, socketId: socket.id, userId: user.id },
          "operation delivery failed",
        );
      if (typeof acknowledge !== "function") {
        app.log.warn(
          { socketId: socket.id, userId: user.id },
          "operation rejected because acknowledgement callback is required",
        );
        return;
      }
      if (Date.now() - windowStarted >= 10_000) {
        windowStarted = Date.now();
        commandsInWindow = 0;
      }
      commandsInWindow += 1;
      if (commandsInWindow > 100) {
        acknowledgeWithoutThrow(
          acknowledge,
          {
            ok: false,
            code: "RATE_LIMITED",
            message: "Durable command rate exceeded",
            retryable: true,
          },
          reportDeliveryFailure,
        );
        return;
      }
      try {
        if (JSON.stringify(raw).length > 64 * 1024) {
          acknowledgeWithoutThrow(
            acknowledge,
            {
              ok: false,
              code: "PAYLOAD_TOO_LARGE",
              message: "Command exceeds 64 KiB",
              retryable: false,
            },
            reportDeliveryFailure,
          );
          return;
        }
        const command = durableCommandSchema.parse(raw);
        const committed = await deliveryCoordinator.run(command.boardId, async () => {
          const result = await repository.commitOperation(user.id, command);
          try {
            await options.deliveryHooks?.afterOperationCommit?.({
              boardId: command.boardId,
              operation: result.operation,
            });
          } catch (error) {
            app.log.warn({ error, boardId: command.boardId }, "operation delivery hook failed");
          }
          publishWithoutThrow(
            () => io.to(boardRoom(command.boardId)).emit("operation:committed", result.operation),
            reportDeliveryFailure,
          );
          return result;
        });
        acknowledgeWithoutThrow(
          acknowledge,
          { ok: true, duplicate: committed.duplicate, operation: committed.operation },
          reportDeliveryFailure,
        );
      } catch (error) {
        app.log.warn({ error, userId: user.id }, "operation rejected");
        acknowledgeWithoutThrow(acknowledge, failedAck(error), reportDeliveryFailure);
      }
    });
  });
  return { app, io };
}
