import type pg from "pg";
import { idSchema } from "@converge/protocol";
import {
  BOARD_SNAPSHOT_MAX_BYTES,
  BoardSnapshotError,
  captureBoardSnapshotInTransaction,
  type BoardSnapshotRepositoryHooks,
} from "./board-snapshot-repository.js";

export const SNAPSHOT_CANDIDATE_SCAN_LIMIT_DEFAULT = 100 as const;
export const SNAPSHOT_CANDIDATE_BATCH_SIZE_DEFAULT = 16 as const;
export const SNAPSHOT_OPERATION_THRESHOLD_DEFAULT = 1_000 as const;
export const SNAPSHOT_CHANGED_AGE_MS_DEFAULT = 86_400_000 as const;
export const SNAPSHOT_OPERATION_BYTES_THRESHOLD_DEFAULT = 8_388_608 as const;
export const SNAPSHOT_MAX_PAYLOAD_BYTES_DEFAULT = 16_777_216 as const;

export type SnapshotEligibilityReason =
  | "bootstrap"
  | "invalid_replacement"
  | "operation_count"
  | "operation_bytes"
  | "changed_age";

export interface SnapshotCandidate {
  boardId: string;
  canvasHead: number;
  deliveryHead: number;
  verifiedSnapshotCanvasHead: number | null;
  verifiedSnapshotDeliveryHead: number | null;
  reason: SnapshotEligibilityReason;
}

export interface SnapshotCandidateDiscoveryResult {
  candidates: SnapshotCandidate[];
  nextCursor: string | null;
  inspectedCount: number;
}

export interface SnapshotEligibilityOptions {
  operationThreshold?: number;
  changedAgeMs?: number;
  operationBytesThreshold?: number;
  currentTime?: Date;
}

export interface SnapshotCandidateDiscoveryOptions extends SnapshotEligibilityOptions {
  cursor?: string | null;
  scanLimit?: number;
  candidateLimit?: number;
}

export interface SnapshotCaptureOptions extends SnapshotEligibilityOptions {
  maximumPayloadBytes?: number;
}

export type SnapshotCaptureOutcome =
  | {
      status: "captured";
      snapshotId: string;
      canvasHead: number;
      deliveryHead: number;
    }
  | { status: "busy" }
  | { status: "no_longer_eligible" }
  | {
      status: "deterministic_failure";
      code:
        | "SNAPSHOT_TOO_LARGE"
        | "INVALID_SOURCE_PROJECTION"
        | "UNSUPPORTED_SNAPSHOT_VERSION"
        | "SNAPSHOT_VERIFICATION_FAILED";
    };

export class SnapshotCandidateError extends Error {
  constructor(
    public readonly code: "INVALID_CURSOR" | "INVALID_CONFIGURATION" | "INVALID_DATABASE_EVIDENCE",
  ) {
    super(`Snapshot candidate selection failed: ${code}`);
  }
}

interface EligibilityPolicy {
  operationThreshold: number;
  changedAgeMs: number;
  operationBytesThreshold: number;
  currentTime: Date | null;
}

interface SnapshotEligibilityRow {
  board_id: string;
  last_seq: string;
  last_delivery_seq: string;
  traversal_group: number;
  verified_snapshot_seq: string | null;
  verified_delivery_seq: string | null;
  verified_at: Date | string | null;
  latest_status: string | null;
  operation_count: string;
  operation_bytes: string;
  effective_now: Date | string;
}

const TIMER_MAXIMUM_MS = 2_147_483_647;

function positiveSafeInteger(value: number, timerSafe = false): number {
  if (!Number.isSafeInteger(value) || value <= 0 || (timerSafe && value > TIMER_MAXIMUM_MS))
    throw new SnapshotCandidateError("INVALID_CONFIGURATION");
  return value;
}

function parsePolicy(options: SnapshotEligibilityOptions): EligibilityPolicy {
  const currentTime = options.currentTime ?? null;
  if (currentTime !== null && !Number.isFinite(currentTime.getTime()))
    throw new SnapshotCandidateError("INVALID_CONFIGURATION");
  return {
    operationThreshold: positiveSafeInteger(
      options.operationThreshold ?? SNAPSHOT_OPERATION_THRESHOLD_DEFAULT,
    ),
    changedAgeMs: positiveSafeInteger(
      options.changedAgeMs ?? SNAPSHOT_CHANGED_AGE_MS_DEFAULT,
      true,
    ),
    operationBytesThreshold: positiveSafeInteger(
      options.operationBytesThreshold ?? SNAPSHOT_OPERATION_BYTES_THRESHOLD_DEFAULT,
    ),
    currentTime,
  };
}

function parseSequence(value: string | null): number | null {
  if (value === null) return null;
  if (!/^(0|[1-9]\d*)$/.test(value)) throw new SnapshotCandidateError("INVALID_DATABASE_EVIDENCE");
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed)) throw new SnapshotCandidateError("INVALID_DATABASE_EVIDENCE");
  return parsed;
}

function parseTimestamp(value: Date | string | null): number | null {
  if (value === null) return null;
  const timestamp = value instanceof Date ? value.getTime() : new Date(value).getTime();
  if (!Number.isFinite(timestamp)) throw new SnapshotCandidateError("INVALID_DATABASE_EVIDENCE");
  return timestamp;
}

function eligibilityReason(
  row: SnapshotEligibilityRow,
  policy: EligibilityPolicy,
): SnapshotEligibilityReason | null {
  const canvasHead = parseSequence(row.last_seq);
  const deliveryHead = parseSequence(row.last_delivery_seq);
  const verifiedCanvasHead = parseSequence(row.verified_snapshot_seq);
  const verifiedDeliveryHead = parseSequence(row.verified_delivery_seq);
  const operationCount = parseSequence(row.operation_count);
  const operationBytes = parseSequence(row.operation_bytes);
  const verifiedAt = parseTimestamp(row.verified_at);
  const effectiveNow = parseTimestamp(row.effective_now);
  if (
    canvasHead === null ||
    deliveryHead === null ||
    operationCount === null ||
    operationBytes === null ||
    effectiveNow === null
  )
    throw new SnapshotCandidateError("INVALID_DATABASE_EVIDENCE");

  if (row.latest_status === "invalid") return "invalid_replacement";
  if (verifiedCanvasHead === null || verifiedDeliveryHead === null || verifiedAt === null)
    return "bootstrap";
  if (operationCount >= policy.operationThreshold) return "operation_count";
  if (operationBytes >= policy.operationBytesThreshold) return "operation_bytes";
  if (
    (canvasHead > verifiedCanvasHead || deliveryHead > verifiedDeliveryHead) &&
    effectiveNow - verifiedAt >= policy.changedAgeMs
  )
    return "changed_age";
  return null;
}

function candidateFromRow(
  row: SnapshotEligibilityRow,
  reason: SnapshotEligibilityReason,
): SnapshotCandidate {
  const canvasHead = parseSequence(row.last_seq);
  const deliveryHead = parseSequence(row.last_delivery_seq);
  if (canvasHead === null || deliveryHead === null)
    throw new SnapshotCandidateError("INVALID_DATABASE_EVIDENCE");
  return {
    boardId: row.board_id,
    canvasHead,
    deliveryHead,
    verifiedSnapshotCanvasHead: parseSequence(row.verified_snapshot_seq),
    verifiedSnapshotDeliveryHead: parseSequence(row.verified_delivery_seq),
    reason,
  };
}

async function readEligibilityRows(
  client: pg.Pool | pg.PoolClient,
  policy: EligibilityPolicy,
  cursor: string | null,
  scanLimit: number,
  boardId: string | null,
): Promise<SnapshotEligibilityRow[]> {
  const result = await client.query<SnapshotEligibilityRow>(
    `WITH boundary AS MATERIALIZED (
       SELECT COALESCE($4::timestamptz, clock_timestamp()) AS effective_now
     ),
     after_cursor AS MATERIALIZED (
       SELECT b.id, b.last_seq, b.last_delivery_seq, 0 AS traversal_group
       FROM boards b
       WHERE ($5::uuid IS NULL OR b.id = $5::uuid)
         AND ($1::uuid IS NULL OR b.id > $1::uuid)
       ORDER BY b.id
       LIMIT $2
     ),
     remaining AS MATERIALIZED (
       SELECT GREATEST($2::bigint - count(*), 0) AS board_limit
       FROM after_cursor
     ),
     wrapped AS MATERIALIZED (
       SELECT b.id, b.last_seq, b.last_delivery_seq, 1 AS traversal_group
       FROM boards b
       WHERE $1::uuid IS NOT NULL
         AND b.id <= $1::uuid
         AND ($5::uuid IS NULL OR b.id = $5::uuid)
       ORDER BY b.id
       LIMIT (SELECT board_limit FROM remaining)
     ),
     scanned AS MATERIALIZED (
       SELECT * FROM after_cursor
       UNION ALL
       SELECT * FROM wrapped
     )
     SELECT scanned.id AS board_id,
            scanned.last_seq,
            scanned.last_delivery_seq,
            scanned.traversal_group,
            verified.snapshot_seq AS verified_snapshot_seq,
            verified.snapshot_delivery_seq AS verified_delivery_seq,
            verified.verified_at,
            latest.status AS latest_status,
            tail.operation_count,
            tail.operation_bytes,
            boundary.effective_now
     FROM scanned
     CROSS JOIN boundary
     LEFT JOIN LATERAL (
       SELECT s.snapshot_seq, s.snapshot_delivery_seq, s.verified_at
       FROM board_snapshots s
       WHERE s.board_id = scanned.id AND s.status = 'verified'
       ORDER BY s.snapshot_seq DESC, s.created_at DESC, s.id DESC
       LIMIT 1
     ) verified ON true
     LEFT JOIN LATERAL (
       SELECT s.status
       FROM board_snapshots s
       WHERE s.board_id = scanned.id
       ORDER BY s.snapshot_seq DESC, s.created_at DESC, s.id DESC
       LIMIT 1
     ) latest ON true
     LEFT JOIN LATERAL (
       SELECT count(*)::text AS operation_count,
              COALESCE(sum(bounded.operation_bytes), 0)::text AS operation_bytes
       FROM (
         -- The retained-byte estimate is PostgreSQL pg_column_size over normalized command JSONB.
         -- Limiting to the operation threshold bounds work: reaching that row count is already eligible.
         SELECT pg_column_size(o.command)::bigint AS operation_bytes
         FROM board_operations o
         WHERE verified.snapshot_seq IS NOT NULL
           AND o.board_id = scanned.id
           AND o.seq > verified.snapshot_seq
         ORDER BY o.seq
         LIMIT $3
       ) bounded
     ) tail ON true
     ORDER BY scanned.traversal_group, scanned.id`,
    [cursor, scanLimit, policy.operationThreshold, policy.currentTime, boardId],
  );
  return result.rows;
}

function isUniqueViolation(error: unknown): boolean {
  return (
    typeof error === "object" &&
    error !== null &&
    "code" in error &&
    (error as { code?: unknown }).code === "23505"
  );
}

function deterministicFailure(error: BoardSnapshotError): SnapshotCaptureOutcome | null {
  switch (error.code) {
    case "SNAPSHOT_TOO_LARGE":
    case "INVALID_SOURCE_PROJECTION":
    case "UNSUPPORTED_SNAPSHOT_VERSION":
      return { status: "deterministic_failure", code: error.code };
    case "SNAPSHOT_CORRUPT":
      return { status: "deterministic_failure", code: "SNAPSHOT_VERIFICATION_FAILED" };
    case "BOARD_NOT_FOUND":
    case "DUPLICATE_SNAPSHOT_HEAD":
      return { status: "no_longer_eligible" };
  }
}

export class BoardSnapshotCandidateRepository {
  constructor(
    private readonly pool: pg.Pool,
    private readonly snapshotHooks: BoardSnapshotRepositoryHooks = {},
  ) {}

  async discover(
    options: SnapshotCandidateDiscoveryOptions = {},
  ): Promise<SnapshotCandidateDiscoveryResult> {
    const cursor = options.cursor ?? null;
    if (cursor !== null && !idSchema.safeParse(cursor).success)
      throw new SnapshotCandidateError("INVALID_CURSOR");
    const scanLimit = positiveSafeInteger(
      options.scanLimit ?? SNAPSHOT_CANDIDATE_SCAN_LIMIT_DEFAULT,
    );
    const candidateLimit = positiveSafeInteger(
      options.candidateLimit ?? SNAPSHOT_CANDIDATE_BATCH_SIZE_DEFAULT,
    );
    const policy = parsePolicy(options);
    const rows = await readEligibilityRows(this.pool, policy, cursor, scanLimit, null);
    const candidates: SnapshotCandidate[] = [];
    for (const row of rows) {
      const reason = eligibilityReason(row, policy);
      if (reason !== null && candidates.length < candidateLimit)
        candidates.push(candidateFromRow(row, reason));
    }
    return {
      candidates,
      nextCursor: rows.at(-1)?.board_id ?? cursor,
      inspectedCount: rows.length,
    };
  }

  async capture(
    boardId: string,
    options: SnapshotCaptureOptions = {},
  ): Promise<SnapshotCaptureOutcome> {
    if (!idSchema.safeParse(boardId).success) throw new SnapshotCandidateError("INVALID_CURSOR");
    const policy = parsePolicy(options);
    const maximumPayloadBytes = positiveSafeInteger(
      options.maximumPayloadBytes ?? SNAPSHOT_MAX_PAYLOAD_BYTES_DEFAULT,
    );
    if (maximumPayloadBytes > BOARD_SNAPSHOT_MAX_BYTES)
      throw new SnapshotCandidateError("INVALID_CONFIGURATION");

    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      const lock = await client.query<{ acquired: boolean }>(
        "SELECT pg_try_advisory_xact_lock(hashtextextended($1, 0)) AS acquired",
        [boardId],
      );
      if (lock.rows[0]?.acquired !== true) {
        await client.query("COMMIT");
        return { status: "busy" };
      }
      await this.snapshotHooks.afterAdvisoryLock?.(boardId);
      const rows = await readEligibilityRows(client, policy, null, 1, boardId);
      const row = rows[0];
      if (!row || eligibilityReason(row, policy) === null) {
        await client.query("COMMIT");
        return { status: "no_longer_eligible" };
      }
      const snapshot = await captureBoardSnapshotInTransaction(
        client,
        boardId,
        this.snapshotHooks,
        maximumPayloadBytes,
      );
      await client.query("COMMIT");
      return {
        status: "captured",
        snapshotId: snapshot.id,
        canvasHead: snapshot.snapshotSeq,
        deliveryHead: snapshot.snapshotDeliverySeq,
      };
    } catch (error) {
      await client.query("ROLLBACK");
      if (isUniqueViolation(error)) return { status: "no_longer_eligible" };
      if (error instanceof BoardSnapshotError) {
        const outcome = deterministicFailure(error);
        if (outcome) return outcome;
      }
      throw error;
    } finally {
      client.release();
    }
  }
}
