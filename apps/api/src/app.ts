import type { Writable } from "node:stream";
import Fastify, { type FastifyInstance } from "fastify";
import cors from "@fastify/cors";
import rateLimit from "@fastify/rate-limit";
import { Server, type DefaultEventsMap } from "socket.io";
import { z } from "zod";
import {
  noOpTelemetryRecorder,
  safeTelemetryRecorder,
  type TelemetryRecorder,
} from "@converge/observability";
import {
  BoardRecoveryError,
  BoardRecoveryMaterialRepository,
  BoardRepository,
  RepositoryError,
  type VerifiedBoardRecoveryMaterial,
  type BoardRepositoryHooks,
  type DatabasePool,
} from "@converge/database";
import {
  MAX_SYNC_BATCH_SIZE,
  boardAccessRevokedEventSchema,
  boardRecoveryMaterialSchema,
  boardRecoveryRequestQuerySchema,
  committedOperationSchema,
  createBoardRequestSchema,
  durableCommandSchema,
  httpInternalErrorResponseSchema,
  joinBoardAckSchema,
  joinBoardRequestSchema,
  membershipRevokedDeliveryEnvelopeSchema,
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
import { BoardRecoveryService, type BoardRecoveryLoadResult } from "./board-recovery-service.js";
export { BoardRecoveryService } from "./board-recovery-service.js";
import { BoardDeliveryCoordinator } from "./board-delivery-coordinator.js";
import {
  BoardDeliveryHeadWatchdog,
  BoardDeliveryHeadWatchdogError,
  defaultBoardDeliveryHeadWatchdogConfiguration,
  type BoardDeliveryHeadWatchdogFactory,
  type BoardDeliveryHeadWatchdogLifecycleEvent,
  type BoardDeliveryHeadWatchdogOwner,
} from "./board-delivery-head-watchdog.js";
import type {
  DeliveryRuntimeFactory,
  DeliveryRuntimeLifecycleEvent,
  DeliveryRuntimeObserver,
  DeliveryRuntimeOwner,
} from "./delivery-runtime.js";
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

const durableRecoveryFailureCodes = new Set<BoardRecoveryError["code"]>([
  "MISSING_BOARD_HEAD",
  "MISSING_REQUIRED_SNAPSHOT",
  "SNAPSHOT_BELOW_RECOVERY_FLOOR",
  "SNAPSHOT_TOO_LARGE",
  "SNAPSHOT_CORRUPT",
  "UNSUPPORTED_SNAPSHOT_VERSION",
  "SNAPSHOT_HEAD_BEYOND_BOARD",
  "TAIL_LIMIT_EXCEEDED",
  "TAIL_GAP",
  "TAIL_ORDER_CONFLICT",
  "WRONG_BOARD_OPERATION",
  "MALFORMED_OPERATION",
  "OPERATION_BEYOND_HEAD",
  "REDUCER_FAILURE",
  "PROJECTION_MISMATCH",
  "CANONICAL_HASH_MISMATCH",
  "NO_COMPLETE_RECOVERY_CHAIN",
]);

function recoveryBlocked(error: BoardRecoveryError): ProtocolError | null {
  if (!durableRecoveryFailureCodes.has(error.code)) return null;
  return protocolErrorSchema.parse({
    ok: false,
    code: "RECOVERY_BLOCKED",
    message: "Authoritative board recovery is unavailable",
    retryable: false,
  });
}

function recoveryResponse(material: VerifiedBoardRecoveryMaterial) {
  return boardRecoveryMaterialSchema.parse({
    boardId: material.boardId,
    snapshotId: material.snapshotId,
    snapshotSchemaVersion: material.snapshotSchemaVersion,
    snapshotCanvasSeq: material.snapshotCanvasSeq,
    snapshotDeliverySeq: material.snapshotDeliverySeq,
    capturedCanvasSeq: material.capturedCanvasSeq,
    capturedDeliverySeq: material.capturedDeliverySeq,
    snapshotState: material.snapshot.projection,
    snapshotCanonicalHash: material.snapshotHash,
    operationTail: material.operations,
    reconstructedCanonicalHash: material.reconstructedHash,
  });
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
    data: protocolErrorSchema.parse(failedAck(error)),
  });
}

function socketDeliveryUnavailableError(): Error {
  return Object.assign(new Error("Realtime delivery is temporarily unavailable"), {
    data: protocolErrorSchema.parse({
      ok: false,
      code: "INTERNAL_ERROR",
      message: "Realtime delivery is temporarily unavailable",
      retryable: true,
    }),
  });
}

export interface AppContext {
  app: FastifyInstance;
  io: AppIo;
}

interface AuthenticatedSocketData {
  principal: AuthenticatedPrincipal;
  revokedBoards: Set<string>;
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

export type ApplicationDeliveryMode =
  | { mode: "local" }
  | {
      mode: "distributed";
      createRuntime: DeliveryRuntimeFactory;
    };

export interface BuildAppOptions {
  synchronizationBatchSize?: number;
  synchronizationHooks?: SynchronizationHooks;
  deliveryCoordinator?: BoardDeliveryCoordinator;
  deliveryHooks?: DeliveryHooks;
  deliveryMode?: ApplicationDeliveryMode;
  createBoardDeliveryHeadWatchdog?: BoardDeliveryHeadWatchdogFactory;
  repositoryHooks?: BoardRepositoryHooks;
  recoveryMaterialRepository?: Pick<BoardRecoveryMaterialRepository, "load"> & {
    loadWithOutcome?(boardId: string): Promise<BoardRecoveryLoadResult>;
  };
  loggerStream?: Writable;
  telemetry?: TelemetryRecorder;
  telemetryClock?: { now(): number };
}

export async function buildApp(
  environment: Environment,
  pool: DatabasePool,
  auth: AuthAdapter,
  options: BuildAppOptions = {},
): Promise<AppContext> {
  const telemetry = safeTelemetryRecorder(options.telemetry ?? noOpTelemetryRecorder);
  const telemetryClock = options.telemetryClock ?? { now: () => performance.now() };
  const telemetryNow = (): number | undefined => {
    try {
      const value = telemetryClock.now();
      return Number.isFinite(value) ? value : undefined;
    } catch {
      return undefined;
    }
  };
  const telemetryTimestamp = (): string => new Date().toISOString();
  const emit = (
    eventName:
      | "delivery.consumer.lifecycle"
      | "delivery.cursor_lost"
      | "delivery.watchdog.divergence"
      | "socket.readiness.changed"
      | "recovery.request.result"
      | "api.lifecycle",
    component: "delivery_consumer" | "delivery_watchdog" | "socket_readiness" | "recovery" | "api",
    severity: "info" | "warn" | "error",
    code: string,
  ): void =>
    telemetry.emit({
      schemaVersion: 1,
      eventName,
      component,
      severity,
      timestamp: telemetryTimestamp(),
      code,
    });
  let apiLifecycle: "starting" | "ready" | "stopping" | "stopped" | "startup_failed" = "starting";
  emit("api.lifecycle", "api", "info", "STARTING");
  const transitionApiLifecycle = (next: typeof apiLifecycle): void => {
    if (apiLifecycle === next || apiLifecycle === "stopped") return;
    apiLifecycle = next;
    emit(
      "api.lifecycle",
      "api",
      next === "startup_failed" ? "error" : next === "stopping" ? "info" : "info",
      next.toUpperCase(),
    );
  };
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
  const boardRecoveryMaterialRepository = new BoardRecoveryMaterialRepository(pool);
  const recoveryMaterialRepository =
    options.recoveryMaterialRepository ?? new BoardRecoveryService(boardRecoveryMaterialRepository);
  const deliveryCoordinator = options.deliveryCoordinator ?? new BoardDeliveryCoordinator();
  const deliveryMode = options.deliveryMode ?? { mode: "local" as const };
  telemetry.setGauge("converge_delivery_consumer_ready", {}, 0);
  telemetry.setGauge("converge_socket_ready", {}, deliveryMode.mode === "local" ? 1 : 0);
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
      return reply.code(400).send(
        protocolErrorSchema.parse({
          ok: false,
          code: "INVALID_COMMAND",
          message: "Request validation failed",
          retryable: false,
        }),
      );
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
        const result = await repository.getOperationBatch(
          boardId,
          user.id,
          range.after,
          range.watermark,
          synchronizationBatchSize,
        );
        if ("outcome" in result)
          throw new RepositoryError("RESYNC_REQUIRED", "Operation range is unavailable");
        return {
          ...operationRangeResponseSchema.parse(result),
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
  app.get<{ Params: { boardId: string }; Querystring: Record<string, unknown> }>(
    "/v1/boards/:boardId/recovery",
    async (request, reply) => {
      const startedAt = telemetryNow();
      let outcome:
        | "snapshot_tail"
        | "refreshed"
        | "recovery_blocked"
        | "retryable_failure"
        | "authorization_failure" = "retryable_failure";
      try {
        const user = await authenticateHttp(request);
        const boardId = z.string().uuid().parse(request.params.boardId);
        boardRecoveryRequestQuerySchema.parse(request.query);
        const role = await repository.roleFor(boardId, user.id);
        if (!role) {
          outcome = "authorization_failure";
          throw new RepositoryError("BOARD_NOT_FOUND", "Board not found");
        }
        const loaded = recoveryMaterialRepository.loadWithOutcome
          ? await recoveryMaterialRepository.loadWithOutcome(boardId)
          : {
              material: await recoveryMaterialRepository.load(boardId),
              outcome: "snapshot_tail" as const,
            };
        outcome = loaded.outcome;
        return recoveryResponse(loaded.material);
      } catch (error) {
        if (error instanceof AuthenticationError || error instanceof RepositoryError)
          outcome = "authorization_failure";
        if (error instanceof BoardRecoveryError) {
          const response = recoveryBlocked(error);
          if (response) {
            outcome = "recovery_blocked";
            return reply.code(409).send(response);
          }
        }
        throw error;
      } finally {
        const finishedAt = telemetryNow();
        const elapsed =
          startedAt !== undefined && finishedAt !== undefined ? finishedAt - startedAt : 0;
        telemetry.increment("converge_recovery_requests_total", { outcome });
        telemetry.observe(
          "converge_recovery_duration_seconds",
          {},
          Number.isFinite(elapsed) && elapsed >= 0 ? elapsed / 1_000 : 0,
        );
        emit(
          "recovery.request.result",
          "recovery",
          outcome === "snapshot_tail" || outcome === "refreshed" ? "info" : "warn",
          outcome.toUpperCase(),
        );
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
  const trackedBoards = new Map<string, { references: number; handledDeliverySeq: number }>();
  const socketBoards = new Map<string, Set<string>>();
  const socketTracksBoard = (socketId: string, boardId: string): boolean =>
    socketBoards.get(socketId)?.has(boardId) ?? false;
  const trackBoardJoin = (socketId: string, boardId: string, baseline?: number): void => {
    if (socketTracksBoard(socketId, boardId)) return;
    let board = trackedBoards.get(boardId);
    if (!board) {
      if (baseline === undefined || !Number.isSafeInteger(baseline) || baseline < 0)
        throw new Error(
          "An authoritative delivery baseline is required for first board activation",
        );
      if (trackedBoards.size >= defaultBoardDeliveryHeadWatchdogConfiguration.maximumActiveBoards)
        throw new BoardDeliveryHeadWatchdogError("ACTIVE_BOARD_CAPACITY_EXCEEDED");
      board = { references: 0, handledDeliverySeq: baseline };
      trackedBoards.set(boardId, board);
    }
    if (!Number.isSafeInteger(board.references + 1))
      throw new BoardDeliveryHeadWatchdogError("ACTIVE_BOARD_CAPACITY_EXCEEDED");
    board.references += 1;
    const memberships = socketBoards.get(socketId) ?? new Set<string>();
    memberships.add(boardId);
    socketBoards.set(socketId, memberships);
  };
  const releaseBoardJoin = (socketId: string, boardId: string): void => {
    const memberships = socketBoards.get(socketId);
    if (!memberships?.delete(boardId)) return;
    if (memberships.size === 0) socketBoards.delete(socketId);
    const board = trackedBoards.get(boardId);
    if (!board) return;
    if (board.references <= 1) trackedBoards.delete(boardId);
    else board.references -= 1;
  };
  const releaseSocketBoards = (socketId: string): void => {
    const memberships = [...(socketBoards.get(socketId) ?? [])];
    for (const boardId of memberships) releaseBoardJoin(socketId, boardId);
  };
  const advanceDeliveryProgress = (boardId: string, deliverySeq: number): void => {
    const board = trackedBoards.get(boardId);
    if (!board || !Number.isSafeInteger(deliverySeq) || deliverySeq < 0) return;
    board.handledDeliverySeq = Math.max(board.handledDeliverySeq, deliverySeq);
  };
  const clearTrackedBoards = (): void => {
    socketBoards.clear();
    trackedBoards.clear();
  };
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
        target.data.revokedBoards.add(boardId);
        target.emit("board:access-revoked", event);
        await target.leave(room);
        releaseBoardJoin(target.id, boardId);
      }),
    );
  };

  let deliveryRuntime: DeliveryRuntimeOwner | undefined;
  let deliveryRuntimeStartPromise: Promise<void> | undefined;
  let deliveryRuntimeStopPromise: Promise<void> | undefined;
  let boardDeliveryHeadWatchdog: BoardDeliveryHeadWatchdogOwner | undefined;
  let boardDeliveryHeadWatchdogStartPromise: Promise<void> | undefined;
  let boardDeliveryHeadWatchdogStopPromise: Promise<void> | undefined;
  let socketIoClosePromise: Promise<void> | undefined;
  let distributedSocketReady = deliveryMode.mode === "local";
  let consumerSocketReady = deliveryMode.mode === "local";
  let watchdogSocketReady = true;
  let distributedReadinessTerminal = false;
  let applicationClosing = false;
  let acceptsDistributedDeliveries = false;
  let consumerTelemetryState:
    | "established"
    | "unavailable"
    | "recovering"
    | "recovered"
    | "terminal"
    | undefined;
  let watchdogTelemetryState: "unavailable" | "recovered" | undefined;
  let consumerReadyGauge = 0;
  let socketReadyGauge = deliveryMode.mode === "local" ? 1 : 0;
  const socketsAreReady = (): boolean =>
    deliveryMode.mode === "local" ||
    (!applicationClosing && !distributedReadinessTerminal && distributedSocketReady);
  const requireSocketReadiness = (): void => {
    if (!socketsAreReady()) throw socketDeliveryUnavailableError();
  };
  const refreshSocketReadiness = (): void => {
    if (deliveryMode.mode === "local") return;
    const wasReady = distributedSocketReady;
    distributedSocketReady =
      !applicationClosing &&
      !distributedReadinessTerminal &&
      consumerSocketReady &&
      watchdogSocketReady;
    acceptsDistributedDeliveries = distributedSocketReady;
    const nextGauge = distributedSocketReady ? 1 : 0;
    if (nextGauge !== socketReadyGauge) {
      socketReadyGauge = nextGauge;
      telemetry.setGauge("converge_socket_ready", {}, nextGauge);
      telemetry.increment("converge_delivery_state_transitions_total", {
        source: "socket_readiness",
        state: nextGauge === 1 ? "established" : "unavailable",
      });
      emit(
        "socket.readiness.changed",
        "socket_readiness",
        nextGauge === 1 ? "info" : "warn",
        nextGauge === 1 ? "READY" : "UNAVAILABLE",
      );
    }
    if (wasReady && !distributedSocketReady) io.local.disconnectSockets(true);
  };
  const makeSocketsTerminallyUnready = (): void => {
    if (deliveryMode.mode === "local") return;
    distributedReadinessTerminal = true;
    consumerSocketReady = false;
    refreshSocketReadiness();
  };
  const observeDeliveryLifecycle = (event: DeliveryRuntimeLifecycleEvent): void => {
    if (deliveryMode.mode === "local" || applicationClosing || distributedReadinessTerminal) return;
    const transitionState = event.state === "stopped" ? "terminal" : event.state;
    if (consumerTelemetryState !== transitionState) {
      consumerTelemetryState = transitionState;
      telemetry.increment("converge_delivery_state_transitions_total", {
        source: "consumer",
        state: transitionState,
      });
      emit(
        event.state === "terminal" && event.source === "cursor"
          ? "delivery.cursor_lost"
          : "delivery.consumer.lifecycle",
        "delivery_consumer",
        transitionState === "established" || transitionState === "recovered" ? "info" : "warn",
        event.state === "terminal" ? event.code : event.state.toUpperCase(),
      );
    }
    switch (event.state) {
      case "established":
      case "recovered":
        if (distributedReadinessTerminal) return;
        consumerSocketReady = true;
        if (consumerReadyGauge !== 1) {
          consumerReadyGauge = 1;
          telemetry.setGauge("converge_delivery_consumer_ready", {}, 1);
        }
        refreshSocketReadiness();
        return;
      case "unavailable":
      case "recovering":
        consumerSocketReady = false;
        if (consumerReadyGauge !== 0) {
          consumerReadyGauge = 0;
          telemetry.setGauge("converge_delivery_consumer_ready", {}, 0);
        }
        refreshSocketReadiness();
        return;
      case "terminal":
      case "stopped":
        if (consumerReadyGauge !== 0) {
          consumerReadyGauge = 0;
          telemetry.setGauge("converge_delivery_consumer_ready", {}, 0);
        }
        makeSocketsTerminallyUnready();
        return;
    }
  };
  const deliveryRuntimeObserver: DeliveryRuntimeObserver = {
    lifecycle: observeDeliveryLifecycle,
    quarantine: () => Promise.resolve(),
  };
  const observeWatchdogLifecycle = (event: BoardDeliveryHeadWatchdogLifecycleEvent): void => {
    if (deliveryMode.mode === "local" || applicationClosing || distributedReadinessTerminal) return;
    if (watchdogTelemetryState !== event.state) {
      watchdogTelemetryState = event.state;
      telemetry.increment("converge_delivery_state_transitions_total", {
        source: "watchdog",
        state: event.state,
      });
      if (event.state === "unavailable")
        emit("delivery.watchdog.divergence", "delivery_watchdog", "warn", event.code);
    }
    watchdogSocketReady = event.state === "recovered";
    refreshSocketReadiness();
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
        if (deliveryMode.mode === "local") await evictBoardMember(params.boardId, params.userId);
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
    if (!socketsAreReady()) {
      next(socketDeliveryUnavailableError());
      return;
    }
    void (async () => {
      try {
        const principal = await auth.authenticateSocket(socket);
        requireSocketReadiness();
        if (!principal)
          throw new AuthenticationError("AUTHENTICATION_REQUIRED", "Authentication required");
        socket.data.principal = principal;
        socket.data.revokedBoards = new Set();
        next();
      } catch (error) {
        if (!socketsAreReady()) next(socketDeliveryUnavailableError());
        else if (error instanceof AuthenticationError) next(socketAuthenticationError(error));
        else next(new Error("Authentication failed"));
      }
    })();
  });
  io.on("connection", (socket) => {
    if (!socketsAreReady()) {
      socket.disconnect(true);
      return;
    }
    const user = socket.data.principal;
    let windowStarted = Date.now();
    let commandsInWindow = 0;
    socket.on("disconnect", () => releaseSocketBoards(socket.id));
    socket.on("board:join", async (raw, acknowledge) => {
      if (!socketsAreReady()) return;
      if (typeof acknowledge !== "function") {
        app.log.warn({ socketId: socket.id }, "board join requires an acknowledgement callback");
        return;
      }
      try {
        requireSocketReadiness();
        const request = joinBoardRequestSchema.parse(raw);
        const room = boardRoom(request.boardId);
        let newlyTracked = false;
        let newlyJoined = false;
        let joinWatermark: number;
        try {
          await deliveryCoordinator.run(request.boardId, async () => {
            requireSocketReadiness();
            if (socket.data.revokedBoards.has(request.boardId))
              throw new RepositoryError("FORBIDDEN", "Board access was revoked");
            const role = await repository.roleFor(request.boardId, user.id);
            requireSocketReadiness();
            if (!role) throw new RepositoryError("FORBIDDEN", "Board membership required");
            const alreadyTracked = socketTracksBoard(socket.id, request.boardId);
            let deliveryBaseline: number | undefined;
            if (
              deliveryMode.mode === "distributed" &&
              !alreadyTracked &&
              !trackedBoards.has(request.boardId)
            ) {
              const [head] = await repository.getBoardDeliveryHeads([request.boardId]);
              requireSocketReadiness();
              deliveryBaseline = head?.lastDeliverySeq;
            }
            newlyTracked = deliveryMode.mode === "distributed" && !alreadyTracked;
            newlyJoined = !socket.rooms.has(room);
            if (newlyTracked) trackBoardJoin(socket.id, request.boardId, deliveryBaseline);
            await socket.join(room);
          });
          await options.synchronizationHooks?.afterRoomJoin?.({
            boardId: request.boardId,
            userId: user.id,
            socketId: socket.id,
          });
          requireSocketReadiness();
          joinWatermark = await deliveryCoordinator.run(request.boardId, async () => {
            requireSocketReadiness();
            const watermark = await repository.getBoardSequence(request.boardId, user.id);
            requireSocketReadiness();
            if (request.lastAppliedSeq > watermark)
              throw new RepositoryError(
                "RESYNC_REQUIRED",
                "Client sequence exceeds authoritative board head",
              );
            return watermark;
          });
        } catch (error) {
          if (newlyJoined) await Promise.resolve(socket.leave(room)).catch(() => undefined);
          if (newlyTracked) releaseBoardJoin(socket.id, request.boardId);
          throw error;
        }
        acknowledge(
          joinBoardAckSchema.parse({
            ok: true,
            boardId: request.boardId,
            joinWatermark,
          }),
        );
      } catch (error) {
        if (!socketsAreReady()) return;
        acknowledge(joinBoardAckSchema.parse(failedAck(error)));
      }
    });
    socket.on("operation:submit", async (raw, acknowledge) => {
      if (!socketsAreReady()) return;
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
        requireSocketReadiness();
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
          requireSocketReadiness();
          if (socket.data.revokedBoards.has(command.boardId))
            throw new RepositoryError("FORBIDDEN", "Board access was revoked");
          const result = await repository.commitOperation(user.id, command);
          try {
            await options.deliveryHooks?.afterOperationCommit?.({
              boardId: command.boardId,
              operation: result.operation,
            });
          } catch (error) {
            app.log.warn({ error, boardId: command.boardId }, "operation delivery hook failed");
          }
          if (deliveryMode.mode === "local")
            publishWithoutThrow(
              () => io.to(boardRoom(command.boardId)).emit("operation:committed", result.operation),
              reportDeliveryFailure,
            );
          return result;
        });
        if (socketsAreReady() && !socket.data.revokedBoards.has(command.boardId))
          acknowledgeWithoutThrow(
            acknowledge,
            { ok: true, duplicate: committed.duplicate, operation: committed.operation },
            reportDeliveryFailure,
          );
      } catch (error) {
        app.log.warn({ error, userId: user.id }, "operation rejected");
        if (!socketsAreReady()) return;
        acknowledgeWithoutThrow(acknowledge, failedAck(error), reportDeliveryFailure);
      }
    });
  });

  const stopDeliveryRuntimeOnce = (): Promise<void> => {
    deliveryRuntimeStopPromise ??= deliveryRuntime?.stop() ?? Promise.resolve();
    return deliveryRuntimeStopPromise;
  };
  const stopBoardDeliveryHeadWatchdogOnce = (): Promise<void> => {
    boardDeliveryHeadWatchdogStopPromise ??= boardDeliveryHeadWatchdog?.stop() ?? Promise.resolve();
    return boardDeliveryHeadWatchdogStopPromise;
  };
  const closeSocketIoOnce = (): Promise<void> => {
    socketIoClosePromise ??= io.close();
    return socketIoClosePromise;
  };
  app.addHook("onReady", async () => {
    if (!deliveryRuntime) {
      transitionApiLifecycle("ready");
      return;
    }
    try {
      boardDeliveryHeadWatchdogStartPromise ??=
        boardDeliveryHeadWatchdog?.start() ?? Promise.resolve();
      await boardDeliveryHeadWatchdogStartPromise;
      deliveryRuntimeStartPromise ??= deliveryRuntime.start();
      await deliveryRuntimeStartPromise;
      transitionApiLifecycle("ready");
    } catch (error) {
      transitionApiLifecycle("startup_failed");
      makeSocketsTerminallyUnready();
      await stopDeliveryRuntimeOnce().catch(() => undefined);
      await stopBoardDeliveryHeadWatchdogOnce().catch(() => undefined);
      throw error;
    }
  });
  app.addHook("preClose", async () => {
    applicationClosing = true;
    transitionApiLifecycle("stopping");
    consumerReadyGauge = 0;
    telemetry.setGauge("converge_delivery_consumer_ready", {}, 0);
    makeSocketsTerminallyUnready();
    socketReadyGauge = 0;
    telemetry.setGauge("converge_socket_ready", {}, 0);
    await stopDeliveryRuntimeOnce();
    await stopBoardDeliveryHeadWatchdogOnce();
    clearTrackedBoards();
    await closeSocketIoOnce();
  });
  app.addHook("onClose", () => {
    transitionApiLifecycle("stopped");
  });

  if (deliveryMode.mode === "distributed") {
    if (typeof deliveryMode.createRuntime !== "function") {
      transitionApiLifecycle("startup_failed");
      await app.close();
      throw new TypeError("Distributed delivery mode requires a runtime factory");
    }
    try {
      const createWatchdog =
        options.createBoardDeliveryHeadWatchdog ??
        ((input) =>
          new BoardDeliveryHeadWatchdog(
            input.repository,
            input.activeBoards,
            input.deliveryProgress,
            input.observer,
          ));
      boardDeliveryHeadWatchdog = createWatchdog({
        repository,
        activeBoards: { activeBoardIds: () => trackedBoards.keys() },
        deliveryProgress: {
          handledDeliverySequence: (boardId) => trackedBoards.get(boardId)?.handledDeliverySeq ?? 0,
        },
        observer: { lifecycle: observeWatchdogLifecycle },
      });
      deliveryRuntime = deliveryMode.createRuntime(
        {
          operationCommitted: (envelope) => {
            if (!acceptsDistributedDeliveries) return Promise.resolve();
            return deliveryCoordinator.run(envelope.boardId, () => {
              if (!acceptsDistributedDeliveries) return Promise.resolve();
              const operation = committedOperationSchema.parse(envelope.payload.operation);
              if (operation.boardId !== envelope.boardId)
                throw new Error("Committed operation board does not match its delivery envelope");
              io.local.to(boardRoom(envelope.boardId)).emit("operation:committed", operation);
              advanceDeliveryProgress(envelope.boardId, envelope.deliverySeq);
              return Promise.resolve();
            });
          },
          membershipRevoked: (envelope) => {
            if (!acceptsDistributedDeliveries) return Promise.resolve();
            return deliveryCoordinator.run(envelope.boardId, () => {
              if (!acceptsDistributedDeliveries) return Promise.resolve();
              const revocation = membershipRevokedDeliveryEnvelopeSchema.parse(envelope);
              return evictBoardMember(revocation.boardId, revocation.payload.revokedUserId).then(
                () => {
                  if (acceptsDistributedDeliveries)
                    advanceDeliveryProgress(revocation.boardId, revocation.deliverySeq);
                },
              );
            });
          },
        },
        deliveryRuntimeObserver,
        telemetry,
      );
    } catch (error) {
      transitionApiLifecycle("startup_failed");
      await app.close();
      throw error;
    }
  }
  return { app, io };
}
