import type pg from "pg";
import { hashBoardState, reduceCommand, type BoardState } from "@converge/canvas-engine";
import {
  committedOperationSchema,
  durableCommandSchema,
  idSchema,
  type CommittedOperation,
} from "@converge/protocol";
import {
  BOARD_SNAPSHOT_MAX_BYTES,
  BOARD_SNAPSHOT_SCHEMA_VERSION,
  BoardSnapshotError,
  canonicalBoardSnapshot,
  captureBoardSnapshotInTransaction,
  hashBoardSnapshot,
  parseBoardSnapshotProjection,
  verifyStoredBoardSnapshot,
  type BoardSnapshotProjection,
  type BoardSnapshotStorageRow,
  type SnapshotProjectedObject,
  type VerifiedBoardSnapshot,
} from "./board-snapshot-repository.js";

export const DEFAULT_RECOVERY_TAIL_LIMIT = 100 as const;
export const MAX_RECOVERY_TAIL_LIMIT = 100 as const;

export class BoardRecoveryError extends Error {
  constructor(
    public readonly code:
      | "INVALID_CONFIGURATION"
      | "MISSING_BOARD_HEAD"
      | "MISSING_REQUIRED_SNAPSHOT"
      | "SNAPSHOT_BELOW_RECOVERY_FLOOR"
      | "SNAPSHOT_TOO_LARGE"
      | "SNAPSHOT_CORRUPT"
      | "UNSUPPORTED_SNAPSHOT_VERSION"
      | "SNAPSHOT_HEAD_BEYOND_BOARD"
      | "TAIL_LIMIT_EXCEEDED"
      | "TAIL_GAP"
      | "TAIL_ORDER_CONFLICT"
      | "WRONG_BOARD_OPERATION"
      | "MALFORMED_OPERATION"
      | "OPERATION_BEYOND_HEAD"
      | "REDUCER_FAILURE"
      | "PROJECTION_MISMATCH"
      | "CANONICAL_HASH_MISMATCH"
      | "NO_COMPLETE_RECOVERY_CHAIN"
      | "SNAPSHOT_INVALIDATION_FAILED",
  ) {
    super(`Board recovery blocked: ${code}`);
  }
}

export class BoardRecoveryRefreshInfrastructureError extends Error {
  constructor(public readonly code: "BOARD_LOCK_BUSY") {
    super(`Board recovery refresh unavailable: ${code}`);
  }
}

export const DEFAULT_RECOVERY_REFRESH_TIMEOUT_MS = 5_000 as const;

export interface BoardRecoveryMaterialRepositoryHooks {
  afterAdvisoryLock?: (boardId: string) => Promise<void>;
  afterBoundaryCaptured?: (boundary: {
    boardId: string;
    canvasSeq: number;
    deliverySeq: number;
  }) => Promise<void>;
  transformTailEvidence?: (
    rows: readonly RecoveryOperationEvidence[],
  ) => Promise<readonly RecoveryOperationEvidence[]>;
}

export interface RecoveryOperationEvidence {
  board_id: unknown;
  seq: unknown;
  command: unknown;
  committed_at: unknown;
}

export interface VerifiedBoardRecoveryMaterial {
  boardId: string;
  snapshotId: string;
  snapshotSchemaVersion: typeof BOARD_SNAPSHOT_SCHEMA_VERSION;
  snapshotCanvasSeq: number;
  snapshotDeliverySeq: number;
  capturedCanvasSeq: number;
  capturedDeliverySeq: number;
  snapshot: VerifiedBoardSnapshot;
  snapshotHash: string;
  operations: readonly CommittedOperation[];
  reconstructedState: BoardState;
  reconstructedHash: string;
  reconstructedProjectionHash: string;
}

interface HeadRow {
  id: unknown;
  name: unknown;
  last_seq: unknown;
  last_delivery_seq: unknown;
  operation_recovery_floor: unknown;
  delivery_recovery_floor: unknown;
}

interface ProjectionRow {
  object_id: unknown;
  stack_order: unknown;
  object_data: unknown;
  field_seq: unknown;
  created_seq: unknown;
  updated_seq: unknown;
  deleted_seq: unknown;
}

function parseSequence(value: unknown, positive = false): number {
  const parsed =
    typeof value === "number"
      ? value
      : typeof value === "string" && /^(0|[1-9]\d*)$/.test(value)
        ? Number(value)
        : Number.NaN;
  if (!Number.isSafeInteger(parsed) || parsed < (positive ? 1 : 0))
    throw new BoardRecoveryError("MISSING_BOARD_HEAD");
  return parsed;
}

function stateFromProjection(projection: BoardSnapshotProjection): {
  state: BoardState;
  stackOrder: Map<string, number>;
} {
  const state: BoardState = { lastSeq: projection.lastSeq, objects: {}, order: [] };
  const stackOrder = new Map<string, number>();
  for (const object of projection.objects) {
    state.objects[object.objectId] = {
      value: object.value,
      fieldSeq: { ...object.fieldSeq },
      createdSeq: object.createdSeq,
      updatedSeq: object.updatedSeq,
      deletedSeq: object.deletedSeq,
    };
    stackOrder.set(object.objectId, object.stackOrder);
    if (object.deletedSeq === null) state.order.push(object.objectId);
  }
  return { state, stackOrder };
}

function projectionFromState(
  snapshot: VerifiedBoardSnapshot,
  state: BoardState,
  stackOrder: ReadonlyMap<string, number>,
  deliverySeq: number,
): BoardSnapshotProjection {
  const objects: SnapshotProjectedObject[] = Object.entries(state.objects)
    .map(([objectId, object]) => {
      const order = stackOrder.get(objectId);
      if (order === undefined) throw new BoardRecoveryError("PROJECTION_MISMATCH");
      return {
        objectId,
        stackOrder: order,
        value: object.value,
        fieldSeq: { ...object.fieldSeq },
        createdSeq: object.createdSeq,
        updatedSeq: object.updatedSeq,
        deletedSeq: object.deletedSeq,
      };
    })
    .sort((left, right) =>
      left.stackOrder === right.stackOrder
        ? left.objectId.localeCompare(right.objectId)
        : left.stackOrder - right.stackOrder,
    );
  return parseBoardSnapshotProjection({
    schemaVersion: BOARD_SNAPSHOT_SCHEMA_VERSION,
    boardId: snapshot.boardId,
    boardName: snapshot.projection.boardName,
    lastSeq: state.lastSeq,
    lastDeliverySeq: deliverySeq,
    objects,
  });
}

function snapshotInvalidationCode(error: unknown): string {
  return error instanceof BoardSnapshotError && error.code === "UNSUPPORTED_SNAPSHOT_VERSION"
    ? "UNSUPPORTED_VERSION"
    : "CORRUPT_SNAPSHOT";
}

function recoverySnapshotError(error: unknown): BoardRecoveryError {
  return new BoardRecoveryError(
    error instanceof BoardSnapshotError && error.code === "UNSUPPORTED_SNAPSHOT_VERSION"
      ? "UNSUPPORTED_SNAPSHOT_VERSION"
      : "SNAPSHOT_CORRUPT",
  );
}

export class BoardRecoveryMaterialRepository {
  private readonly tailLimit: number;

  constructor(
    private readonly pool: pg.Pool,
    options: {
      tailLimit?: number;
      refreshTimeoutMs?: number;
      maximumSnapshotBytes?: number;
      hooks?: BoardRecoveryMaterialRepositoryHooks;
    } = {},
  ) {
    const tailLimit = options.tailLimit ?? DEFAULT_RECOVERY_TAIL_LIMIT;
    if (!Number.isSafeInteger(tailLimit) || tailLimit <= 0 || tailLimit > MAX_RECOVERY_TAIL_LIMIT)
      throw new BoardRecoveryError("INVALID_CONFIGURATION");
    this.tailLimit = tailLimit;
    const refreshTimeoutMs = options.refreshTimeoutMs ?? DEFAULT_RECOVERY_REFRESH_TIMEOUT_MS;
    if (
      !Number.isSafeInteger(refreshTimeoutMs) ||
      refreshTimeoutMs <= 0 ||
      refreshTimeoutMs > 2_147_483_647
    )
      throw new BoardRecoveryError("INVALID_CONFIGURATION");
    this.refreshTimeoutMs = refreshTimeoutMs;
    const maximumSnapshotBytes = options.maximumSnapshotBytes ?? BOARD_SNAPSHOT_MAX_BYTES;
    if (
      !Number.isSafeInteger(maximumSnapshotBytes) ||
      maximumSnapshotBytes <= 0 ||
      maximumSnapshotBytes > BOARD_SNAPSHOT_MAX_BYTES
    )
      throw new BoardRecoveryError("INVALID_CONFIGURATION");
    this.maximumSnapshotBytes = maximumSnapshotBytes;
    this.hooks = options.hooks ?? {};
  }

  private readonly hooks: BoardRecoveryMaterialRepositoryHooks;
  private readonly refreshTimeoutMs: number;
  private readonly maximumSnapshotBytes: number;

  async load(boardId: string): Promise<VerifiedBoardRecoveryMaterial> {
    if (!idSchema.safeParse(boardId).success) throw new BoardRecoveryError("MISSING_BOARD_HEAD");
    const client = await this.pool.connect();
    let transactionFinished = false;
    try {
      await client.query("BEGIN");
      await client.query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", [boardId]);
      await this.hooks.afterAdvisoryLock?.(boardId);
      try {
        const material = await this.selectRecoveryMaterial(client, boardId);
        await client.query("COMMIT");
        transactionFinished = true;
        return material;
      } catch (error) {
        if (error instanceof BoardRecoveryError && error.code !== "SNAPSHOT_INVALIDATION_FAILED") {
          await client.query("COMMIT");
          transactionFinished = true;
        }
        throw error;
      }
    } catch (error) {
      if (!transactionFinished) await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }

  async refresh(boardId: string): Promise<VerifiedBoardRecoveryMaterial> {
    if (!idSchema.safeParse(boardId).success) throw new BoardRecoveryError("MISSING_BOARD_HEAD");
    const client = await this.pool.connect();
    let transactionFinished = false;
    try {
      await client.query("BEGIN");
      await client.query("SELECT set_config('statement_timeout', $1, true)", [
        String(this.refreshTimeoutMs),
      ]);
      const lock = await client.query<{ acquired: boolean }>(
        "SELECT pg_try_advisory_xact_lock(hashtextextended($1, 0)) AS acquired",
        [boardId],
      );
      if (lock.rows[0]?.acquired !== true) {
        await client.query("COMMIT");
        transactionFinished = true;
        throw new BoardRecoveryRefreshInfrastructureError("BOARD_LOCK_BUSY");
      }
      await this.hooks.afterAdvisoryLock?.(boardId);
      try {
        const material = await this.selectRecoveryMaterial(client, boardId);
        await client.query("COMMIT");
        transactionFinished = true;
        return material;
      } catch (error) {
        if (
          !(error instanceof BoardRecoveryError) ||
          (error.code !== "MISSING_REQUIRED_SNAPSHOT" && error.code !== "TAIL_LIMIT_EXCEEDED")
        ) {
          if (
            error instanceof BoardRecoveryError &&
            error.code !== "SNAPSHOT_INVALIDATION_FAILED"
          ) {
            await client.query("COMMIT");
            transactionFinished = true;
          }
          throw error;
        }
      }

      let snapshot: VerifiedBoardSnapshot;
      try {
        snapshot = await captureBoardSnapshotInTransaction(
          client,
          boardId,
          {},
          this.maximumSnapshotBytes,
        );
      } catch (error) {
        if (error instanceof BoardSnapshotError) throw this.captureRecoveryError(error);
        throw error;
      }
      const material = await this.selectRecoveryMaterial(client, boardId);
      if (material.snapshotId !== snapshot.id || material.operations.length !== 0)
        throw new BoardRecoveryError("NO_COMPLETE_RECOVERY_CHAIN");
      await client.query("COMMIT");
      transactionFinished = true;
      return material;
    } catch (error) {
      if (!transactionFinished) await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }

  private async selectRecoveryMaterial(
    client: pg.PoolClient,
    boardId: string,
  ): Promise<VerifiedBoardRecoveryMaterial> {
    const headResult = await client.query<HeadRow>(
      `SELECT id, name, last_seq, last_delivery_seq,
              operation_recovery_floor, delivery_recovery_floor
       FROM boards WHERE id = $1 FOR UPDATE`,
      [boardId],
    );
    const head = headResult.rows[0];
    if (
      !head ||
      head.id !== boardId ||
      typeof head.name !== "string" ||
      head.name.length < 1 ||
      head.name.length > 120
    )
      throw new BoardRecoveryError("MISSING_BOARD_HEAD");
    const capturedCanvasSeq = parseSequence(head.last_seq);
    const capturedDeliverySeq = parseSequence(head.last_delivery_seq);
    const operationRecoveryFloor = parseSequence(head.operation_recovery_floor);
    const deliveryRecoveryFloor = parseSequence(head.delivery_recovery_floor);
    if (
      capturedDeliverySeq < capturedCanvasSeq ||
      operationRecoveryFloor > capturedCanvasSeq ||
      deliveryRecoveryFloor > capturedDeliverySeq
    )
      throw new BoardRecoveryError("MISSING_BOARD_HEAD");
    await this.hooks.afterBoundaryCaptured?.({
      boardId,
      canvasSeq: capturedCanvasSeq,
      deliverySeq: capturedDeliverySeq,
    });

    const currentProjection = await this.loadAuthoritativeProjection(
      client,
      boardId,
      head.name,
      capturedCanvasSeq,
      capturedDeliverySeq,
    );
    const latestSnapshot = await client.query<{ status: unknown }>(
      `SELECT status FROM board_snapshots
         WHERE board_id = $1
         ORDER BY snapshot_seq DESC, created_at DESC, id DESC
         LIMIT 1`,
      [boardId],
    );
    let corruptionObserved = latestSnapshot.rows[0]?.status === "invalid";
    let candidateFound = false;
    const floorIneligible = await client.query<{ present: boolean }>(
      `SELECT EXISTS (
         SELECT 1 FROM board_snapshots
         WHERE board_id = $1
           AND status = 'verified'
           AND (snapshot_seq < $2 OR snapshot_delivery_seq < $3)
       ) AS present`,
      [boardId, operationRecoveryFloor, deliveryRecoveryFloor],
    );
    let beforeSnapshotSeq: number | undefined;
    let finalError: BoardRecoveryError = new BoardRecoveryError("NO_COMPLETE_RECOVERY_CHAIN");
    while (true) {
      const candidateResult = await client.query<BoardSnapshotStorageRow>(
        `SELECT * FROM board_snapshots
           WHERE board_id = $1
             AND status = 'verified'
             AND snapshot_seq >= $2
             AND snapshot_delivery_seq >= $3
             AND ($4::bigint IS NULL OR snapshot_seq < $4)
           ORDER BY snapshot_seq DESC, created_at DESC, id DESC
           LIMIT 1`,
        [boardId, operationRecoveryFloor, deliveryRecoveryFloor, beforeSnapshotSeq ?? null],
      );
      const candidate = candidateResult.rows[0];
      if (!candidate) break;
      candidateFound = true;
      beforeSnapshotSeq = parseSequence(candidate.snapshot_seq);
      let snapshot: VerifiedBoardSnapshot;
      try {
        snapshot = verifyStoredBoardSnapshot(candidate, true);
        if (
          snapshot.boardId !== boardId ||
          snapshot.snapshotSeq > capturedCanvasSeq ||
          snapshot.snapshotDeliverySeq > capturedDeliverySeq
        ) {
          await this.invalidate(client, candidate, boardId, "HEAD_INCONSISTENT");
          corruptionObserved = true;
          finalError = new BoardRecoveryError("SNAPSHOT_HEAD_BEYOND_BOARD");
          continue;
        }
      } catch (error) {
        await this.invalidate(client, candidate, boardId, snapshotInvalidationCode(error));
        corruptionObserved = true;
        finalError = recoverySnapshotError(error);
        continue;
      }

      try {
        const operations = await this.loadTail(
          client,
          boardId,
          snapshot.snapshotSeq,
          capturedCanvasSeq,
        );
        const reconstructed = await this.reconstruct(
          snapshot,
          operations,
          capturedDeliverySeq,
          currentProjection,
        );
        return {
          boardId,
          snapshotId: snapshot.id,
          snapshotSchemaVersion: snapshot.schemaVersion,
          snapshotCanvasSeq: snapshot.snapshotSeq,
          snapshotDeliverySeq: snapshot.snapshotDeliverySeq,
          capturedCanvasSeq,
          capturedDeliverySeq,
          snapshot,
          snapshotHash: snapshot.canonicalHash,
          operations,
          ...reconstructed,
        };
      } catch (error) {
        finalError =
          error instanceof BoardRecoveryError
            ? error
            : new BoardRecoveryError("NO_COMPLETE_RECOVERY_CHAIN");
        if (finalError.code === "TAIL_LIMIT_EXCEEDED") break;
      }
    }

    if (corruptionObserved) throw new BoardRecoveryError("SNAPSHOT_CORRUPT");
    if (!candidateFound)
      throw new BoardRecoveryError(
        floorIneligible.rows[0]?.present === true
          ? "SNAPSHOT_BELOW_RECOVERY_FLOOR"
          : "MISSING_REQUIRED_SNAPSHOT",
      );
    throw finalError;
  }

  private captureRecoveryError(error: BoardSnapshotError): BoardRecoveryError {
    switch (error.code) {
      case "SNAPSHOT_TOO_LARGE":
        return new BoardRecoveryError("SNAPSHOT_TOO_LARGE");
      case "UNSUPPORTED_SNAPSHOT_VERSION":
        return new BoardRecoveryError("UNSUPPORTED_SNAPSHOT_VERSION");
      case "INVALID_SOURCE_PROJECTION":
        return new BoardRecoveryError("PROJECTION_MISMATCH");
      case "SNAPSHOT_CORRUPT":
        return new BoardRecoveryError("SNAPSHOT_CORRUPT");
      case "BOARD_NOT_FOUND":
        return new BoardRecoveryError("MISSING_BOARD_HEAD");
      case "DUPLICATE_SNAPSHOT_HEAD":
        return new BoardRecoveryError("NO_COMPLETE_RECOVERY_CHAIN");
    }
  }

  private async invalidate(
    client: pg.PoolClient,
    candidate: BoardSnapshotStorageRow,
    boardId: string,
    code: string,
  ): Promise<void> {
    const result = await client.query(
      `UPDATE board_snapshots
       SET status = 'invalid', invalidation_code = $3, invalidated_at = clock_timestamp()
       WHERE id = $1 AND board_id = $2 AND status = 'verified'`,
      [candidate.id, boardId, code],
    );
    if (result.rowCount !== 1) throw new BoardRecoveryError("SNAPSHOT_INVALIDATION_FAILED");
  }

  private async loadTail(
    client: pg.PoolClient,
    boardId: string,
    snapshotSeq: number,
    capturedSeq: number,
  ): Promise<CommittedOperation[]> {
    const expectedCount = capturedSeq - snapshotSeq;
    if (!Number.isSafeInteger(expectedCount) || expectedCount < 0)
      throw new BoardRecoveryError("SNAPSHOT_HEAD_BEYOND_BOARD");
    if (expectedCount > this.tailLimit) throw new BoardRecoveryError("TAIL_LIMIT_EXCEEDED");
    const result = await client.query<RecoveryOperationEvidence>(
      `SELECT board_id, seq, command, committed_at
       FROM board_operations
       WHERE board_id = $1 AND seq > $2
       ORDER BY seq
       LIMIT $3`,
      [boardId, snapshotSeq, this.tailLimit + 1],
    );
    const rows = this.hooks.transformTailEvidence
      ? await this.hooks.transformTailEvidence(result.rows)
      : result.rows;
    if (rows.length > expectedCount) {
      const extraSequence = parseSequence(rows[expectedCount]?.seq, true);
      if (extraSequence > capturedSeq) throw new BoardRecoveryError("OPERATION_BEYOND_HEAD");
      throw new BoardRecoveryError("TAIL_ORDER_CONFLICT");
    }
    const operations: CommittedOperation[] = [];
    let expected = snapshotSeq + 1;
    for (const row of rows) {
      if (row.board_id !== boardId) throw new BoardRecoveryError("WRONG_BOARD_OPERATION");
      let sequence: number;
      try {
        sequence = parseSequence(row.seq, true);
      } catch {
        throw new BoardRecoveryError("MALFORMED_OPERATION");
      }
      if (sequence > capturedSeq) throw new BoardRecoveryError("OPERATION_BEYOND_HEAD");
      if (sequence < expected) throw new BoardRecoveryError("TAIL_ORDER_CONFLICT");
      if (sequence > expected) throw new BoardRecoveryError("TAIL_GAP");
      try {
        const command = durableCommandSchema.parse(row.command);
        if (command.boardId !== boardId) throw new BoardRecoveryError("WRONG_BOARD_OPERATION");
        const committedAt =
          row.committed_at instanceof Date ? row.committed_at.toISOString() : undefined;
        operations.push(committedOperationSchema.parse({ ...command, seq: sequence, committedAt }));
      } catch (error) {
        if (error instanceof BoardRecoveryError) throw error;
        throw new BoardRecoveryError("MALFORMED_OPERATION");
      }
      expected += 1;
    }
    if (operations.length !== expectedCount || expected - 1 !== capturedSeq)
      throw new BoardRecoveryError("TAIL_GAP");
    return operations;
  }

  private async loadAuthoritativeProjection(
    client: pg.PoolClient,
    boardId: string,
    boardName: string,
    canvasSeq: number,
    deliverySeq: number,
  ): Promise<BoardSnapshotProjection> {
    const result = await client.query<ProjectionRow>(
      `SELECT object_id, stack_order, object_data, field_seq, created_seq, updated_seq, deleted_seq
       FROM board_objects
       WHERE board_id = $1
       ORDER BY stack_order, object_id`,
      [boardId],
    );
    try {
      return parseBoardSnapshotProjection({
        schemaVersion: BOARD_SNAPSHOT_SCHEMA_VERSION,
        boardId,
        boardName,
        lastSeq: canvasSeq,
        lastDeliverySeq: deliverySeq,
        objects: result.rows.map((row) => ({
          objectId: row.object_id,
          stackOrder: parseSequence(row.stack_order, true),
          value: row.object_data,
          fieldSeq: row.field_seq,
          createdSeq: parseSequence(row.created_seq, true),
          updatedSeq: parseSequence(row.updated_seq, true),
          deletedSeq: row.deleted_seq === null ? null : parseSequence(row.deleted_seq, true),
        })),
      });
    } catch {
      throw new BoardRecoveryError("PROJECTION_MISMATCH");
    }
  }

  private async reconstruct(
    snapshot: VerifiedBoardSnapshot,
    operations: readonly CommittedOperation[],
    deliverySeq: number,
    currentProjection: BoardSnapshotProjection,
  ): Promise<{
    reconstructedState: BoardState;
    reconstructedHash: string;
    reconstructedProjectionHash: string;
  }> {
    const reconstructed = stateFromProjection(snapshot.projection);
    let state = reconstructed.state;
    for (const operation of operations) {
      const wasKnown = state.objects[operation.targetId] !== undefined;
      const reduced = reduceCommand(state, operation, operation.seq);
      if (!reduced.ok) throw new BoardRecoveryError("REDUCER_FAILURE");
      state = reduced.state;
      if (!wasKnown && operation.type === "object.create")
        reconstructed.stackOrder.set(operation.targetId, operation.seq);
    }
    if (state.lastSeq !== currentProjection.lastSeq)
      throw new BoardRecoveryError("PROJECTION_MISMATCH");
    const reconstructedProjection = projectionFromState(
      snapshot,
      state,
      reconstructed.stackOrder,
      deliverySeq,
    );
    if (
      canonicalBoardSnapshot(reconstructedProjection) !== canonicalBoardSnapshot(currentProjection)
    )
      throw new BoardRecoveryError("PROJECTION_MISMATCH");
    const reconstructedProjectionHash = hashBoardSnapshot(reconstructedProjection);
    if (reconstructedProjectionHash !== hashBoardSnapshot(currentProjection))
      throw new BoardRecoveryError("CANONICAL_HASH_MISMATCH");
    const authoritativeState = stateFromProjection(currentProjection).state;
    const [reconstructedHash, authoritativeHash] = await Promise.all([
      hashBoardState(state),
      hashBoardState(authoritativeState),
    ]);
    if (reconstructedHash !== authoritativeHash)
      throw new BoardRecoveryError("CANONICAL_HASH_MISMATCH");
    return { reconstructedState: state, reconstructedHash, reconstructedProjectionHash };
  }
}
