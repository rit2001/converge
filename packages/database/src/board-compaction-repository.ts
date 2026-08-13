import type pg from "pg";
import { idSchema } from "@converge/protocol";
import {
  verifyStoredBoardSnapshot,
  type BoardSnapshotStorageRow,
  type VerifiedBoardSnapshot,
} from "./board-snapshot-repository.js";

export const COMPACTION_SNAPSHOT_SAFETY_GENERATIONS = 1 as const;

export type BoardCompactionBlockedCode =
  | "SNAPSHOT_BEYOND_BOARD_HEAD"
  | "SNAPSHOT_BEHIND_RECOVERY_FLOOR"
  | "OPERATION_RANGE_GAP"
  | "OPERATION_RECEIPT_EVIDENCE_INVALID"
  | "OPERATION_TAIL_GAP"
  | "OUTBOX_RANGE_GAP"
  | "OUTBOX_PUBLICATION_EVIDENCE_INVALID";

export interface BoardCompactionBoundary {
  boardId: string;
  previousOperationFloor: number;
  newOperationFloor: number;
  previousDeliveryFloor: number;
  newDeliveryFloor: number;
  deletedOperationCount: number;
  deletedOutboxCount: number;
  snapshotId: string;
  snapshotCanvasSeq: number;
  snapshotDeliverySeq: number;
}

export type BoardCompactionResult =
  | ({ outcome: "compacted" | "no_progress" } & BoardCompactionBoundary)
  | { outcome: "no_verified_boundary"; boardId: string }
  | {
      outcome: "blocked";
      boardId: string;
      code: BoardCompactionBlockedCode;
      snapshotId: string;
      snapshotCanvasSeq: number;
      snapshotDeliverySeq: number;
    };

export class BoardCompactionError extends Error {
  constructor(public readonly code: "BOARD_NOT_FOUND" | "INVALID_BOARD_ID") {
    super(`Board compaction failed: ${code}`);
  }
}

export interface BoardCompactionRepositoryHooks {
  afterAdvisoryLock?: (boardId: string) => Promise<void>;
  afterDeletionsBeforeFloorUpdate?: (boundary: BoardCompactionBoundary) => Promise<void>;
}

interface BoardHeadRow {
  last_seq: unknown;
  last_delivery_seq: unknown;
  operation_recovery_floor: unknown;
  delivery_recovery_floor: unknown;
}

interface EvidenceSummaryRow {
  total_count: string;
  valid_count: string;
}

function parseSequence(value: unknown): number {
  const parsed =
    typeof value === "number"
      ? value
      : typeof value === "string" && /^(0|[1-9]\d*)$/.test(value)
        ? Number(value)
        : Number.NaN;
  if (!Number.isSafeInteger(parsed) || parsed < 0)
    throw new Error("Database returned an invalid board compaction sequence");
  return parsed;
}

function parseCount(value: unknown): number {
  const parsed = parseSequence(value);
  if (!Number.isSafeInteger(parsed))
    throw new Error("Database returned an invalid board compaction count");
  return parsed;
}

function blocked(
  boardId: string,
  snapshot: VerifiedBoardSnapshot,
  code: BoardCompactionBlockedCode,
): BoardCompactionResult {
  return {
    outcome: "blocked",
    boardId,
    code,
    snapshotId: snapshot.id,
    snapshotCanvasSeq: snapshot.snapshotSeq,
    snapshotDeliverySeq: snapshot.snapshotDeliverySeq,
  };
}

function boundaryResult(
  boardId: string,
  snapshot: VerifiedBoardSnapshot,
  previousOperationFloor: number,
  previousDeliveryFloor: number,
  deletedOperationCount = 0,
  deletedOutboxCount = 0,
): BoardCompactionBoundary {
  return {
    boardId,
    previousOperationFloor,
    newOperationFloor: snapshot.snapshotSeq,
    previousDeliveryFloor,
    newDeliveryFloor: snapshot.snapshotDeliverySeq,
    deletedOperationCount,
    deletedOutboxCount,
    snapshotId: snapshot.id,
    snapshotCanvasSeq: snapshot.snapshotSeq,
    snapshotDeliverySeq: snapshot.snapshotDeliverySeq,
  };
}

export class BoardCompactionRepository {
  constructor(
    private readonly pool: pg.Pool,
    private readonly hooks: BoardCompactionRepositoryHooks = {},
  ) {}

  async compact(boardIdInput: string): Promise<BoardCompactionResult> {
    const parsedBoardId = idSchema.safeParse(boardIdInput);
    if (!parsedBoardId.success) throw new BoardCompactionError("INVALID_BOARD_ID");
    const boardId = parsedBoardId.data;
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      await client.query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", [boardId]);
      await this.hooks.afterAdvisoryLock?.(boardId);

      const boardResult = await client.query<BoardHeadRow>(
        `SELECT last_seq, last_delivery_seq,
                operation_recovery_floor, delivery_recovery_floor
         FROM boards WHERE id = $1 FOR UPDATE`,
        [boardId],
      );
      const board = boardResult.rows[0];
      if (!board) throw new BoardCompactionError("BOARD_NOT_FOUND");
      const canvasHead = parseSequence(board.last_seq);
      const deliveryHead = parseSequence(board.last_delivery_seq);
      const previousOperationFloor = parseSequence(board.operation_recovery_floor);
      const previousDeliveryFloor = parseSequence(board.delivery_recovery_floor);

      const snapshotResult = await client.query<BoardSnapshotStorageRow>(
        `SELECT * FROM board_snapshots
         WHERE board_id = $1 AND status = 'verified'
         ORDER BY snapshot_seq DESC, created_at DESC, id DESC
         LIMIT $2
         FOR UPDATE`,
        [boardId, COMPACTION_SNAPSHOT_SAFETY_GENERATIONS + 1],
      );
      if (snapshotResult.rows.length <= COMPACTION_SNAPSHOT_SAFETY_GENERATIONS) {
        await client.query("COMMIT");
        return { outcome: "no_verified_boundary", boardId };
      }

      for (const row of snapshotResult.rows) verifyStoredBoardSnapshot(row, true);
      const candidateRow = snapshotResult.rows[COMPACTION_SNAPSHOT_SAFETY_GENERATIONS];
      if (!candidateRow) throw new Error("Compaction snapshot selection failed");
      const snapshot = verifyStoredBoardSnapshot(candidateRow, true);
      if (
        snapshot.boardId !== boardId ||
        snapshot.snapshotSeq > canvasHead ||
        snapshot.snapshotDeliverySeq > deliveryHead
      ) {
        await client.query("COMMIT");
        return blocked(boardId, snapshot, "SNAPSHOT_BEYOND_BOARD_HEAD");
      }
      if (
        snapshot.snapshotSeq < previousOperationFloor ||
        snapshot.snapshotDeliverySeq < previousDeliveryFloor
      ) {
        await client.query("COMMIT");
        return blocked(boardId, snapshot, "SNAPSHOT_BEHIND_RECOVERY_FLOOR");
      }

      const proposed = boundaryResult(
        boardId,
        snapshot,
        previousOperationFloor,
        previousDeliveryFloor,
      );
      if (
        snapshot.snapshotSeq === previousOperationFloor &&
        snapshot.snapshotDeliverySeq === previousDeliveryFloor
      ) {
        await client.query("COMMIT");
        return { outcome: "no_progress", ...proposed };
      }

      const expectedOperationCount = snapshot.snapshotSeq - previousOperationFloor;
      const operationEvidence = await client.query<EvidenceSummaryRow>(
        `SELECT count(*)::text AS total_count,
                count(*) FILTER (WHERE
                  receipt.operation_id IS NOT NULL
                  AND receipt.actor_id = operation.user_id
                  AND receipt.command = operation.command
                  AND receipt.canvas_seq = operation.seq
                  AND receipt.delivery_seq = operation.delivery_seq
                  AND receipt.event_id = operation.event_id
                  AND receipt.committed_at = operation.committed_at
                  AND converge_operation_receipt_is_valid(
                    receipt.command, receipt.result, receipt.board_id, receipt.operation_id,
                    receipt.canvas_seq, receipt.delivery_seq, receipt.event_id,
                    receipt.committed_at, receipt.hash_schema_version, receipt.command_hash
                  ) IS TRUE
                  AND event.id IS NOT NULL
                  AND event.board_id = operation.board_id
                  AND event.delivery_seq = operation.delivery_seq
                  AND event.canvas_seq = operation.seq
                  AND event.event_type = 'operation.committed'
                  AND event.schema_version = 1
                  AND event.payload = receipt.result
                )::text AS valid_count
         FROM board_operations operation
         LEFT JOIN board_operation_receipts receipt
           ON receipt.board_id = operation.board_id
          AND receipt.operation_id = operation.op_id
         LEFT JOIN outbox_events event ON event.id = operation.event_id
         WHERE operation.board_id = $1
           AND operation.seq > $2
           AND operation.seq <= $3`,
        [boardId, previousOperationFloor, snapshot.snapshotSeq],
      );
      const operationSummary = operationEvidence.rows[0];
      if (!operationSummary) throw new Error("Operation evidence summary is unavailable");
      const operationCount = parseCount(operationSummary.total_count);
      const validOperationCount = parseCount(operationSummary.valid_count);
      if (operationCount !== expectedOperationCount) {
        await client.query("COMMIT");
        return blocked(boardId, snapshot, "OPERATION_RANGE_GAP");
      }
      if (validOperationCount !== expectedOperationCount) {
        await client.query("COMMIT");
        return blocked(boardId, snapshot, "OPERATION_RECEIPT_EVIDENCE_INVALID");
      }

      const operationTail = await client.query<{ tail_count: string }>(
        `SELECT count(*)::text AS tail_count
         FROM board_operations
         WHERE board_id = $1 AND seq > $2 AND seq <= $3`,
        [boardId, snapshot.snapshotSeq, canvasHead],
      );
      if (parseCount(operationTail.rows[0]?.tail_count) !== canvasHead - snapshot.snapshotSeq) {
        await client.query("COMMIT");
        return blocked(boardId, snapshot, "OPERATION_TAIL_GAP");
      }

      const expectedOutboxCount = snapshot.snapshotDeliverySeq - previousDeliveryFloor;
      const outboxEvidence = await client.query<EvidenceSummaryRow>(
        `SELECT count(*)::text AS total_count,
                count(*) FILTER (WHERE
                  event.status = 'published'
                  AND event.attempt_count BETWEEN 0 AND 20
                  AND event.lease_owner IS NULL
                  AND event.lease_token IS NULL
                  AND event.leased_until IS NULL
                  AND event.next_attempt_at = 'infinity'::timestamptz
                  AND event.redis_entry_id IS NOT NULL
                  AND char_length(event.redis_entry_id) BETWEEN 1 AND 128
                  AND event.redis_entry_id !~ '[[:cntrl:]]'
                  AND event.published_at IS NOT NULL
                  AND event.published_at > '-infinity'::timestamptz
                  AND event.published_at < 'infinity'::timestamptz
                  AND event.last_error_code IS NULL
                  AND event.last_error_message IS NULL
                  AND event.last_error_at IS NULL
                  AND event.schema_version = 1
                  AND jsonb_typeof(event.payload) = 'object'
                  AND event.payload ?& ARRAY[
                    'schemaVersion', 'eventId', 'boardId', 'deliverySeq',
                    'eventType', 'occurredAt', 'payload'
                  ]::text[]
                  AND event.payload - ARRAY[
                    'schemaVersion', 'eventId', 'boardId', 'deliverySeq',
                    'eventType', 'occurredAt', 'payload'
                  ]::text[] = '{}'::jsonb
                  AND jsonb_typeof(event.payload->'schemaVersion') = 'number'
                  AND event.payload->>'schemaVersion' = '1'
                  AND jsonb_typeof(event.payload->'eventId') = 'string'
                  AND event.payload->>'eventId' = event.id::text
                  AND jsonb_typeof(event.payload->'boardId') = 'string'
                  AND event.payload->>'boardId' = event.board_id::text
                  AND jsonb_typeof(event.payload->'deliverySeq') = 'number'
                  AND event.payload->>'deliverySeq' = event.delivery_seq::text
                  AND jsonb_typeof(event.payload->'eventType') = 'string'
                  AND event.payload->>'eventType' = event.event_type
                  AND jsonb_typeof(event.payload->'occurredAt') = 'string'
                  AND event.payload->>'occurredAt' ~
                    '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\\.[0-9]+)?(Z|[+-][0-9]{2}:[0-9]{2})$'
                  AND jsonb_typeof(event.payload->'payload') = 'object'
                  AND CASE event.event_type
                    WHEN 'operation.committed' THEN
                      event.canvas_seq IS NOT NULL
                      AND EXISTS (
                        SELECT 1 FROM board_operation_receipts receipt
                        WHERE receipt.event_id = event.id
                          AND receipt.board_id = event.board_id
                          AND receipt.delivery_seq = event.delivery_seq
                          AND receipt.canvas_seq = event.canvas_seq
                          AND receipt.result = event.payload
                          AND converge_operation_receipt_is_valid(
                            receipt.command, receipt.result, receipt.board_id,
                            receipt.operation_id, receipt.canvas_seq, receipt.delivery_seq,
                            receipt.event_id, receipt.committed_at,
                            receipt.hash_schema_version, receipt.command_hash
                          ) IS TRUE
                      )
                    WHEN 'board.membership.revoked' THEN
                      event.canvas_seq IS NULL
                      AND event.payload->'payload' ?&
                        ARRAY['revokedUserId', 'initiatedByUserId']::text[]
                      AND (event.payload->'payload') -
                        ARRAY['revokedUserId', 'initiatedByUserId']::text[] = '{}'::jsonb
                      AND jsonb_typeof(event.payload->'payload'->'revokedUserId') = 'string'
                      AND event.payload->'payload'->>'revokedUserId' ~*
                        '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                      AND jsonb_typeof(event.payload->'payload'->'initiatedByUserId') = 'string'
                      AND event.payload->'payload'->>'initiatedByUserId' ~*
                        '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                    ELSE FALSE
                  END
                )::text AS valid_count
         FROM outbox_events event
         WHERE event.board_id = $1
           AND event.delivery_seq > $2
           AND event.delivery_seq <= $3`,
        [boardId, previousDeliveryFloor, snapshot.snapshotDeliverySeq],
      );
      const outboxSummary = outboxEvidence.rows[0];
      if (!outboxSummary) throw new Error("Outbox evidence summary is unavailable");
      const outboxCount = parseCount(outboxSummary.total_count);
      const validOutboxCount = parseCount(outboxSummary.valid_count);
      if (outboxCount !== expectedOutboxCount) {
        await client.query("COMMIT");
        return blocked(boardId, snapshot, "OUTBOX_RANGE_GAP");
      }
      if (validOutboxCount !== expectedOutboxCount) {
        await client.query("COMMIT");
        return blocked(boardId, snapshot, "OUTBOX_PUBLICATION_EVIDENCE_INVALID");
      }

      const deletedOperations = await client.query(
        `DELETE FROM board_operations
         WHERE board_id = $1 AND seq > $2 AND seq <= $3`,
        [boardId, previousOperationFloor, snapshot.snapshotSeq],
      );
      const deletedOutbox = await client.query(
        `DELETE FROM outbox_events
         WHERE board_id = $1
           AND delivery_seq > $2
           AND delivery_seq <= $3
           AND status = 'published'`,
        [boardId, previousDeliveryFloor, snapshot.snapshotDeliverySeq],
      );
      if (
        deletedOperations.rowCount !== expectedOperationCount ||
        deletedOutbox.rowCount !== expectedOutboxCount
      )
        throw new Error("Compaction deletion count changed after safety validation");

      const completed = boundaryResult(
        boardId,
        snapshot,
        previousOperationFloor,
        previousDeliveryFloor,
        deletedOperations.rowCount,
        deletedOutbox.rowCount,
      );
      await this.hooks.afterDeletionsBeforeFloorUpdate?.(completed);
      const updated = await client.query(
        `UPDATE boards
         SET operation_recovery_floor = $2,
             delivery_recovery_floor = $3
         WHERE id = $1
           AND operation_recovery_floor = $4
           AND delivery_recovery_floor = $5`,
        [
          boardId,
          snapshot.snapshotSeq,
          snapshot.snapshotDeliverySeq,
          previousOperationFloor,
          previousDeliveryFloor,
        ],
      );
      if (updated.rowCount !== 1) throw new Error("Compaction floor update lost its board fence");
      await client.query("COMMIT");
      return { outcome: "compacted", ...completed };
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }
}
