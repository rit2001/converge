import type pg from "pg";
import { idSchema } from "@converge/protocol";

export const COMPACTION_CANDIDATE_SCAN_LIMIT_MAXIMUM = 100 as const;
export const COMPACTION_CANDIDATE_RESULT_LIMIT_MAXIMUM = 16 as const;

export interface BoardCompactionCandidate {
  boardId: string;
  operationRecoveryFloor: number;
  deliveryRecoveryFloor: number;
  snapshotId: string;
  snapshotCanvasSeq: number;
  snapshotDeliverySeq: number;
  canvasHead: number;
  deliveryHead: number;
}

export interface BoardCompactionCandidateDiscoveryOptions {
  cursor: string | null;
  scanLimit: number;
  resultLimit: number;
}

export interface BoardCompactionCandidateDiscoveryResult {
  candidates: readonly BoardCompactionCandidate[];
  nextCursor: string | null;
  inspectedCount: number;
}

export class BoardCompactionCandidateError extends Error {
  constructor(
    public readonly code: "INVALID_CURSOR" | "INVALID_CONFIGURATION" | "INVALID_DATABASE_EVIDENCE",
  ) {
    super(`Board compaction candidate discovery failed: ${code}`);
  }
}

interface CandidateEvidenceRow {
  board_id: unknown;
  last_seq: unknown;
  last_delivery_seq: unknown;
  operation_recovery_floor: unknown;
  delivery_recovery_floor: unknown;
  traversal_group: unknown;
  newest_snapshot_id: unknown;
  newest_snapshot_seq: unknown;
  newest_snapshot_delivery_seq: unknown;
  proposed_snapshot_id: unknown;
  proposed_snapshot_seq: unknown;
  proposed_snapshot_delivery_seq: unknown;
}

function positiveBoundedInteger(value: number, maximum: number): number {
  if (!Number.isSafeInteger(value) || value <= 0 || value > maximum)
    throw new BoardCompactionCandidateError("INVALID_CONFIGURATION");
  return value;
}

function parseId(value: unknown): string {
  const parsed = idSchema.safeParse(value);
  if (!parsed.success) throw new BoardCompactionCandidateError("INVALID_DATABASE_EVIDENCE");
  return parsed.data;
}

function parseSequence(value: unknown): number {
  const parsed =
    typeof value === "number"
      ? value
      : typeof value === "string" && /^(0|[1-9]\d*)$/.test(value)
        ? Number(value)
        : Number.NaN;
  if (!Number.isSafeInteger(parsed) || parsed < 0)
    throw new BoardCompactionCandidateError("INVALID_DATABASE_EVIDENCE");
  return parsed;
}

function parseOptionalGeneration(
  id: unknown,
  canvasSeq: unknown,
  deliverySeq: unknown,
): { id: string; canvasSeq: number; deliverySeq: number } | null {
  const missing = [id, canvasSeq, deliverySeq].filter((value) => value === null).length;
  if (missing === 3) return null;
  if (missing !== 0) throw new BoardCompactionCandidateError("INVALID_DATABASE_EVIDENCE");
  const generation = {
    id: parseId(id),
    canvasSeq: parseSequence(canvasSeq),
    deliverySeq: parseSequence(deliverySeq),
  };
  if (generation.deliverySeq < generation.canvasSeq)
    throw new BoardCompactionCandidateError("INVALID_DATABASE_EVIDENCE");
  return generation;
}

function candidateFromRow(row: CandidateEvidenceRow): BoardCompactionCandidate | null {
  const boardId = parseId(row.board_id);
  const canvasHead = parseSequence(row.last_seq);
  const deliveryHead = parseSequence(row.last_delivery_seq);
  const operationRecoveryFloor = parseSequence(row.operation_recovery_floor);
  const deliveryRecoveryFloor = parseSequence(row.delivery_recovery_floor);
  if (
    (row.traversal_group !== 0 && row.traversal_group !== 1) ||
    deliveryHead < canvasHead ||
    operationRecoveryFloor > canvasHead ||
    deliveryRecoveryFloor > deliveryHead
  )
    throw new BoardCompactionCandidateError("INVALID_DATABASE_EVIDENCE");

  const newest = parseOptionalGeneration(
    row.newest_snapshot_id,
    row.newest_snapshot_seq,
    row.newest_snapshot_delivery_seq,
  );
  const proposed = parseOptionalGeneration(
    row.proposed_snapshot_id,
    row.proposed_snapshot_seq,
    row.proposed_snapshot_delivery_seq,
  );
  if (proposed !== null && newest === null)
    throw new BoardCompactionCandidateError("INVALID_DATABASE_EVIDENCE");
  if (newest === null) return null;
  if (newest.canvasSeq > canvasHead || newest.deliverySeq > deliveryHead)
    throw new BoardCompactionCandidateError("INVALID_DATABASE_EVIDENCE");
  if (proposed === null) return null;
  if (
    proposed.id === newest.id ||
    proposed.canvasSeq >= newest.canvasSeq ||
    proposed.deliverySeq > newest.deliverySeq ||
    proposed.canvasSeq > canvasHead ||
    proposed.deliverySeq > deliveryHead
  )
    throw new BoardCompactionCandidateError("INVALID_DATABASE_EVIDENCE");
  if (proposed.canvasSeq <= operationRecoveryFloor || proposed.deliverySeq <= deliveryRecoveryFloor)
    return null;
  return {
    boardId,
    operationRecoveryFloor,
    deliveryRecoveryFloor,
    snapshotId: proposed.id,
    snapshotCanvasSeq: proposed.canvasSeq,
    snapshotDeliverySeq: proposed.deliverySeq,
    canvasHead,
    deliveryHead,
  };
}

export class BoardCompactionCandidateRepository {
  constructor(private readonly pool: pg.Pool) {}

  async discover(
    options: BoardCompactionCandidateDiscoveryOptions,
  ): Promise<BoardCompactionCandidateDiscoveryResult> {
    if (options.cursor !== null && !idSchema.safeParse(options.cursor).success)
      throw new BoardCompactionCandidateError("INVALID_CURSOR");
    const scanLimit = positiveBoundedInteger(
      options.scanLimit,
      COMPACTION_CANDIDATE_SCAN_LIMIT_MAXIMUM,
    );
    const resultLimit = positiveBoundedInteger(
      options.resultLimit,
      COMPACTION_CANDIDATE_RESULT_LIMIT_MAXIMUM,
    );
    const result = await this.pool.query<CandidateEvidenceRow>(
      `WITH after_cursor AS MATERIALIZED (
         SELECT b.id, b.last_seq, b.last_delivery_seq,
                b.operation_recovery_floor, b.delivery_recovery_floor,
                0 AS traversal_group
         FROM boards b
         WHERE ($1::uuid IS NULL OR b.id > $1::uuid)
         ORDER BY b.id
         LIMIT $2
       ),
       remaining AS MATERIALIZED (
         SELECT GREATEST($2::bigint - count(*), 0) AS board_limit
         FROM after_cursor
       ),
       wrapped AS MATERIALIZED (
         SELECT b.id, b.last_seq, b.last_delivery_seq,
                b.operation_recovery_floor, b.delivery_recovery_floor,
                1 AS traversal_group
         FROM boards b
         WHERE $1::uuid IS NOT NULL AND b.id <= $1::uuid
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
              scanned.operation_recovery_floor,
              scanned.delivery_recovery_floor,
              scanned.traversal_group,
              generations.newest_snapshot_id,
              generations.newest_snapshot_seq,
              generations.newest_snapshot_delivery_seq,
              generations.proposed_snapshot_id,
              generations.proposed_snapshot_seq,
              generations.proposed_snapshot_delivery_seq
       FROM scanned
       LEFT JOIN LATERAL (
         SELECT
           (array_agg(snapshot.id ORDER BY snapshot.snapshot_seq DESC,
                      snapshot.created_at DESC, snapshot.id DESC))[1] AS newest_snapshot_id,
           (array_agg(snapshot.snapshot_seq ORDER BY snapshot.snapshot_seq DESC,
                      snapshot.created_at DESC, snapshot.id DESC))[1] AS newest_snapshot_seq,
           (array_agg(snapshot.snapshot_delivery_seq ORDER BY snapshot.snapshot_seq DESC,
                      snapshot.created_at DESC, snapshot.id DESC))[1]
             AS newest_snapshot_delivery_seq,
           (array_agg(snapshot.id ORDER BY snapshot.snapshot_seq DESC,
                      snapshot.created_at DESC, snapshot.id DESC))[2] AS proposed_snapshot_id,
           (array_agg(snapshot.snapshot_seq ORDER BY snapshot.snapshot_seq DESC,
                      snapshot.created_at DESC, snapshot.id DESC))[2] AS proposed_snapshot_seq,
           (array_agg(snapshot.snapshot_delivery_seq ORDER BY snapshot.snapshot_seq DESC,
                      snapshot.created_at DESC, snapshot.id DESC))[2]
             AS proposed_snapshot_delivery_seq
         FROM (
           SELECT id, snapshot_seq, snapshot_delivery_seq, created_at
           FROM board_snapshots
           WHERE board_id = scanned.id AND status = 'verified'
           ORDER BY snapshot_seq DESC, created_at DESC, id DESC
           LIMIT 2
         ) snapshot
       ) generations ON true
       ORDER BY scanned.traversal_group, scanned.id`,
      [options.cursor, scanLimit],
    );

    const candidates: BoardCompactionCandidate[] = [];
    let nextCursor = options.cursor;
    for (const row of result.rows) {
      nextCursor = parseId(row.board_id);
      const candidate = candidateFromRow(row);
      if (candidate !== null && candidates.length < resultLimit) candidates.push(candidate);
    }
    return { candidates, nextCursor, inspectedCount: result.rows.length };
  }
}
