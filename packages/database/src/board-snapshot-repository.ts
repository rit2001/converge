import { createHash } from "node:crypto";
import type pg from "pg";
import { canvasObjectSchema, idSchema, type CanvasObject } from "@converge/protocol";

export const BOARD_SNAPSHOT_SCHEMA_VERSION = 1 as const;
export const BOARD_SNAPSHOT_HASH_DOMAIN = "converge.snapshot.v1" as const;
export const BOARD_SNAPSHOT_MAX_BYTES = 16 * 1024 * 1024;

const snapshotKeys = [
  "schemaVersion",
  "boardId",
  "boardName",
  "lastSeq",
  "lastDeliverySeq",
  "objects",
] as const;
const projectedObjectKeys = [
  "objectId",
  "stackOrder",
  "value",
  "fieldSeq",
  "createdSeq",
  "updatedSeq",
  "deletedSeq",
] as const;
const canvasFieldNames = [
  "id",
  "kind",
  "x",
  "y",
  "width",
  "height",
  "rotation",
  "fill",
  "text",
] as const;

export interface SnapshotProjectedObject {
  objectId: string;
  stackOrder: number;
  value: CanvasObject;
  fieldSeq: Record<string, number>;
  createdSeq: number;
  updatedSeq: number;
  deletedSeq: number | null;
}

export interface BoardSnapshotProjection {
  schemaVersion: typeof BOARD_SNAPSHOT_SCHEMA_VERSION;
  boardId: string;
  boardName: string;
  lastSeq: number;
  lastDeliverySeq: number;
  objects: SnapshotProjectedObject[];
}

export interface VerifiedBoardSnapshot {
  id: string;
  boardId: string;
  snapshotSeq: number;
  snapshotDeliverySeq: number;
  schemaVersion: typeof BOARD_SNAPSHOT_SCHEMA_VERSION;
  projection: BoardSnapshotProjection;
  canonicalHash: string;
  objectCount: number;
  byteSize: number;
  createdAt: string;
  verifiedAt: string;
}

export class BoardSnapshotError extends Error {
  constructor(
    public readonly code:
      | "BOARD_NOT_FOUND"
      | "DUPLICATE_SNAPSHOT_HEAD"
      | "INVALID_SOURCE_PROJECTION"
      | "SNAPSHOT_TOO_LARGE"
      | "SNAPSHOT_CORRUPT"
      | "UNSUPPORTED_SNAPSHOT_VERSION",
  ) {
    super(`Board snapshot failed: ${code}`);
  }
}

export interface BoardSnapshotRepositoryHooks {
  afterAdvisoryLock?: (boardId: string) => Promise<void>;
  afterProjectionRead?: (projection: BoardSnapshotProjection) => Promise<void>;
  afterInsert?: (snapshotId: string) => Promise<void>;
}

interface SnapshotRow {
  id: unknown;
  board_id: unknown;
  snapshot_seq: unknown;
  snapshot_delivery_seq: unknown;
  schema_version: unknown;
  projection: unknown;
  canonical_hash: unknown;
  object_count: unknown;
  byte_size: unknown;
  status: unknown;
  created_at: unknown;
  verified_at: unknown;
}

interface ProjectionRow {
  object_id: string;
  stack_order: string;
  object_data: unknown;
  field_seq: unknown;
  created_seq: string;
  updated_seq: string;
  deleted_seq: string | null;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function hasExactKeys(value: Record<string, unknown>, keys: readonly string[]): boolean {
  const actual = Object.keys(value).sort();
  const expected = [...keys].sort();
  return actual.length === expected.length && actual.every((key, index) => key === expected[index]);
}

function parseSafeSequence(value: unknown, positive = false): number {
  const parsed =
    typeof value === "number"
      ? value
      : typeof value === "string" && /^(0|[1-9]\d*)$/.test(value)
        ? Number(value)
        : Number.NaN;
  if (!Number.isSafeInteger(parsed) || parsed < (positive ? 1 : 0))
    throw new BoardSnapshotError("SNAPSHOT_CORRUPT");
  return parsed;
}

function parseFieldSeq(value: unknown, lastSeq: number): Record<string, number> {
  if (!isRecord(value) || !hasExactKeys(value, canvasFieldNames))
    throw new BoardSnapshotError("SNAPSHOT_CORRUPT");
  return Object.fromEntries(
    canvasFieldNames.map((field) => {
      const sequence = parseSafeSequence(value[field], true);
      if (sequence > lastSeq) throw new BoardSnapshotError("SNAPSHOT_CORRUPT");
      return [field, sequence];
    }),
  );
}

export function parseBoardSnapshotProjection(value: unknown): BoardSnapshotProjection {
  if (!isRecord(value) || !hasExactKeys(value, snapshotKeys))
    throw new BoardSnapshotError("SNAPSHOT_CORRUPT");
  if (value.schemaVersion !== BOARD_SNAPSHOT_SCHEMA_VERSION)
    throw new BoardSnapshotError("UNSUPPORTED_SNAPSHOT_VERSION");
  if (!idSchema.safeParse(value.boardId).success) throw new BoardSnapshotError("SNAPSHOT_CORRUPT");
  if (
    typeof value.boardName !== "string" ||
    value.boardName.length < 1 ||
    value.boardName.length > 120
  )
    throw new BoardSnapshotError("SNAPSHOT_CORRUPT");
  const lastSeq = parseSafeSequence(value.lastSeq);
  const lastDeliverySeq = parseSafeSequence(value.lastDeliverySeq);
  if (lastDeliverySeq < lastSeq || !Array.isArray(value.objects))
    throw new BoardSnapshotError("SNAPSHOT_CORRUPT");

  let prior: SnapshotProjectedObject | undefined;
  const objects = value.objects.map((candidate): SnapshotProjectedObject => {
    if (!isRecord(candidate) || !hasExactKeys(candidate, projectedObjectKeys))
      throw new BoardSnapshotError("SNAPSHOT_CORRUPT");
    if (!idSchema.safeParse(candidate.objectId).success)
      throw new BoardSnapshotError("SNAPSHOT_CORRUPT");
    const objectId = candidate.objectId as string;
    const parsedValue = canvasObjectSchema.safeParse(candidate.value);
    if (!parsedValue.success || parsedValue.data.id !== objectId)
      throw new BoardSnapshotError("SNAPSHOT_CORRUPT");
    const stackOrder = parseSafeSequence(candidate.stackOrder, true);
    const createdSeq = parseSafeSequence(candidate.createdSeq, true);
    const updatedSeq = parseSafeSequence(candidate.updatedSeq, true);
    const deletedSeq =
      candidate.deletedSeq === null ? null : parseSafeSequence(candidate.deletedSeq, true);
    if (
      createdSeq > updatedSeq ||
      updatedSeq > lastSeq ||
      (deletedSeq !== null && (deletedSeq !== updatedSeq || deletedSeq > lastSeq)) ||
      (prior !== undefined &&
        (stackOrder < prior.stackOrder ||
          (stackOrder === prior.stackOrder && objectId.localeCompare(prior.objectId) <= 0)))
    )
      throw new BoardSnapshotError("SNAPSHOT_CORRUPT");
    const projected: SnapshotProjectedObject = {
      objectId,
      stackOrder,
      value: parsedValue.data,
      fieldSeq: parseFieldSeq(candidate.fieldSeq, lastSeq),
      createdSeq,
      updatedSeq,
      deletedSeq,
    };
    prior = projected;
    return projected;
  });

  return {
    schemaVersion: BOARD_SNAPSHOT_SCHEMA_VERSION,
    boardId: value.boardId as string,
    boardName: value.boardName,
    lastSeq,
    lastDeliverySeq,
    objects,
  };
}

function canonicalValue(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(canonicalValue);
  if (isRecord(value)) {
    return Object.fromEntries(
      Object.entries(value)
        .filter(([, item]) => item !== undefined)
        .sort(([left], [right]) => (left < right ? -1 : left > right ? 1 : 0))
        .map(([key, item]) => [key, canonicalValue(item)]),
    );
  }
  if (typeof value === "number" && Object.is(value, -0)) return 0;
  return value;
}

export function canonicalBoardSnapshot(projection: BoardSnapshotProjection): string {
  return JSON.stringify(canonicalValue(projection));
}

export function hashBoardSnapshot(projection: BoardSnapshotProjection): string {
  const canonical = canonicalBoardSnapshot(projection);
  return createHash("sha256")
    .update(BOARD_SNAPSHOT_HASH_DOMAIN, "utf8")
    .update("\0", "utf8")
    .update(canonical, "utf8")
    .digest("hex");
}

function parseDate(value: unknown): string {
  const date = value instanceof Date ? value : new Date(String(value));
  if (!Number.isFinite(date.getTime())) throw new BoardSnapshotError("SNAPSHOT_CORRUPT");
  return date.toISOString();
}

function verifySnapshotRow(row: SnapshotRow, requireVerified: boolean): VerifiedBoardSnapshot {
  if (!idSchema.safeParse(row.id).success || !idSchema.safeParse(row.board_id).success)
    throw new BoardSnapshotError("SNAPSHOT_CORRUPT");
  if (row.schema_version !== BOARD_SNAPSHOT_SCHEMA_VERSION)
    throw new BoardSnapshotError("UNSUPPORTED_SNAPSHOT_VERSION");
  const projection = parseBoardSnapshotProjection(row.projection);
  const snapshotSeq = parseSafeSequence(row.snapshot_seq);
  const snapshotDeliverySeq = parseSafeSequence(row.snapshot_delivery_seq);
  const objectCount = parseSafeSequence(row.object_count);
  const byteSize = parseSafeSequence(row.byte_size, true);
  if (
    projection.boardId !== row.board_id ||
    projection.lastSeq !== snapshotSeq ||
    projection.lastDeliverySeq !== snapshotDeliverySeq ||
    projection.schemaVersion !== row.schema_version ||
    projection.objects.length !== objectCount ||
    byteSize !== Buffer.byteLength(canonicalBoardSnapshot(projection), "utf8") ||
    typeof row.canonical_hash !== "string" ||
    !/^[0-9a-f]{64}$/.test(row.canonical_hash) ||
    hashBoardSnapshot(projection) !== row.canonical_hash ||
    (requireVerified && row.status !== "verified") ||
    (!requireVerified && row.status !== "creating" && row.status !== "verified")
  )
    throw new BoardSnapshotError("SNAPSHOT_CORRUPT");
  if (row.verified_at === null || row.verified_at === undefined) {
    if (requireVerified || row.status === "verified")
      throw new BoardSnapshotError("SNAPSHOT_CORRUPT");
  }
  return {
    id: row.id as string,
    boardId: projection.boardId,
    snapshotSeq,
    snapshotDeliverySeq,
    schemaVersion: BOARD_SNAPSHOT_SCHEMA_VERSION,
    projection,
    canonicalHash: row.canonical_hash,
    objectCount,
    byteSize,
    createdAt: parseDate(row.created_at),
    verifiedAt:
      row.verified_at === null || row.verified_at === undefined
        ? parseDate(row.created_at)
        : parseDate(row.verified_at),
  };
}

function isUniqueViolation(error: unknown): boolean {
  return isRecord(error) && error.code === "23505";
}

export class BoardSnapshotRepository {
  constructor(
    private readonly pool: pg.Pool,
    private readonly hooks: BoardSnapshotRepositoryHooks = {},
  ) {}

  async create(boardId: string): Promise<VerifiedBoardSnapshot> {
    if (!idSchema.safeParse(boardId).success)
      throw new BoardSnapshotError("INVALID_SOURCE_PROJECTION");
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      await client.query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", [boardId]);
      await this.hooks.afterAdvisoryLock?.(boardId);
      const board = await client.query<{
        id: string;
        name: string;
        last_seq: string;
        last_delivery_seq: string;
      }>("SELECT id, name, last_seq, last_delivery_seq FROM boards WHERE id = $1 FOR UPDATE", [
        boardId,
      ]);
      const boardRow = board.rows[0];
      if (!boardRow) throw new BoardSnapshotError("BOARD_NOT_FOUND");
      const rows = await client.query<ProjectionRow>(
        `SELECT object_id, stack_order, object_data, field_seq, created_seq, updated_seq, deleted_seq
         FROM board_objects
         WHERE board_id = $1
         ORDER BY stack_order, object_id`,
        [boardId],
      );
      const rawProjection = {
        schemaVersion: BOARD_SNAPSHOT_SCHEMA_VERSION,
        boardId: boardRow.id,
        boardName: boardRow.name,
        lastSeq: parseSafeSequence(boardRow.last_seq),
        lastDeliverySeq: parseSafeSequence(boardRow.last_delivery_seq),
        objects: rows.rows.map((row) => ({
          objectId: row.object_id,
          stackOrder: parseSafeSequence(row.stack_order, true),
          value: row.object_data,
          fieldSeq: row.field_seq,
          createdSeq: parseSafeSequence(row.created_seq, true),
          updatedSeq: parseSafeSequence(row.updated_seq, true),
          deletedSeq: row.deleted_seq === null ? null : parseSafeSequence(row.deleted_seq, true),
        })),
      };
      let projection: BoardSnapshotProjection;
      try {
        projection = parseBoardSnapshotProjection(rawProjection);
      } catch (error) {
        if (error instanceof BoardSnapshotError && error.code === "UNSUPPORTED_SNAPSHOT_VERSION")
          throw error;
        throw new BoardSnapshotError("INVALID_SOURCE_PROJECTION");
      }
      await this.hooks.afterProjectionRead?.(projection);
      const canonical = canonicalBoardSnapshot(projection);
      const byteSize = Buffer.byteLength(canonical, "utf8");
      if (byteSize > BOARD_SNAPSHOT_MAX_BYTES) throw new BoardSnapshotError("SNAPSHOT_TOO_LARGE");
      const snapshotId = crypto.randomUUID();
      await client.query(
        `INSERT INTO board_snapshots(
           id, board_id, snapshot_seq, snapshot_delivery_seq, schema_version, projection,
           canonical_hash, object_count, byte_size, status
         ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,'creating')`,
        [
          snapshotId,
          projection.boardId,
          projection.lastSeq,
          projection.lastDeliverySeq,
          projection.schemaVersion,
          projection,
          hashBoardSnapshot(projection),
          projection.objects.length,
          byteSize,
        ],
      );
      await this.hooks.afterInsert?.(snapshotId);
      const reread = await client.query<SnapshotRow>(
        "SELECT * FROM board_snapshots WHERE id = $1",
        [snapshotId],
      );
      const insertedRow = reread.rows[0];
      if (!insertedRow) throw new BoardSnapshotError("SNAPSHOT_CORRUPT");
      verifySnapshotRow(insertedRow, false);
      const verified = await client.query<SnapshotRow>(
        `UPDATE board_snapshots
         SET status = 'verified', verified_at = clock_timestamp()
         WHERE id = $1 AND status = 'creating'
         RETURNING *`,
        [snapshotId],
      );
      const verifiedRow = verified.rows[0];
      if (!verifiedRow) throw new BoardSnapshotError("SNAPSHOT_CORRUPT");
      const result = verifySnapshotRow(verifiedRow, true);
      await client.query("COMMIT");
      return result;
    } catch (error) {
      await client.query("ROLLBACK");
      if (isUniqueViolation(error)) throw new BoardSnapshotError("DUPLICATE_SNAPSHOT_HEAD");
      throw error;
    } finally {
      client.release();
    }
  }

  async loadLatest(boardId: string): Promise<VerifiedBoardSnapshot | null> {
    if (!idSchema.safeParse(boardId).success) throw new BoardSnapshotError("SNAPSHOT_CORRUPT");
    const result = await this.pool.query<SnapshotRow>(
      `SELECT * FROM board_snapshots
       WHERE board_id = $1
       ORDER BY snapshot_seq DESC, created_at DESC, id DESC
       LIMIT 1`,
      [boardId],
    );
    const row = result.rows[0];
    return row ? verifySnapshotRow(row, true) : null;
  }
}
