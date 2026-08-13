import { execFile } from "node:child_process";
import { promisify } from "node:util";
import type pg from "pg";
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import {
  BoardCompactionCandidateError,
  BoardCompactionCandidateRepository,
  BoardCompactionRepository,
  BoardRepository,
  BoardSnapshotRepository,
  COMPACTION_CANDIDATE_RESULT_LIMIT_MAXIMUM,
  COMPACTION_CANDIDATE_SCAN_LIMIT_MAXIMUM,
  createPool,
} from "@converge/database";
import { createRectangleCommand, fixtureIds } from "@converge/testkit";

const sharedDatabaseUrl =
  process.env.DATABASE_URL ?? "postgresql://converge:converge@localhost:55432/converge";
const repositoryRoot = new URL("../..", import.meta.url);
const runFile = promisify(execFile);
const boardIds = new Set<string>();
let databaseName: string;
let adminPool: ReturnType<typeof createPool>;
let pool: ReturnType<typeof createPool>;
let boards: BoardRepository;
let snapshots: BoardSnapshotRepository;
let discovery: BoardCompactionCandidateRepository;
let compaction: BoardCompactionRepository;

async function createBoard(label: string): Promise<string> {
  const boardId = (await boards.createBoard(fixtureIds.user, `${label}-${crypto.randomUUID()}`)).id;
  boardIds.add(boardId);
  return boardId;
}

async function publish(eventId: string): Promise<void> {
  await pool.query(
    `UPDATE outbox_events
     SET status = 'published', next_attempt_at = 'infinity'::timestamptz,
         redis_entry_id = $2, published_at = clock_timestamp(), updated_at = clock_timestamp()
     WHERE id = $1`,
    [eventId, `candidate-${crypto.randomUUID()}`],
  );
}

async function createGenerations(count: number, label: string, published = false) {
  const boardId = await createBoard(label);
  const generations = [];
  for (let index = 0; index < count; index += 1) {
    const committed = await boards.commitOperation(fixtureIds.user, {
      ...createRectangleCommand(boardId),
      baseSeq: index,
    });
    if (published) await publish(committed.event.eventId);
    generations.push(await snapshots.create(boardId));
  }
  return { boardId, generations };
}

function options(cursor: string | null = null, scanLimit = 100, resultLimit = 16) {
  return { cursor, scanLimit, resultLimit };
}

async function durableSummary(boardId: string): Promise<unknown> {
  const result = await pool.query<{ summary: unknown }>(
    `SELECT jsonb_build_object(
       'board', to_jsonb(b),
       'operations', (SELECT count(*) FROM board_operations WHERE board_id = b.id),
       'outbox', (SELECT count(*) FROM outbox_events WHERE board_id = b.id),
       'receipts', (SELECT count(*) FROM board_operation_receipts WHERE board_id = b.id),
       'snapshots', (SELECT jsonb_agg(to_jsonb(s) ORDER BY snapshot_seq)
                     FROM board_snapshots s WHERE board_id = b.id),
       'objects', (SELECT count(*) FROM board_objects WHERE board_id = b.id),
       'memberships', (SELECT count(*) FROM board_members WHERE board_id = b.id)
     ) AS summary
     FROM boards b WHERE b.id = $1`,
    [boardId],
  );
  return result.rows[0]?.summary;
}

beforeAll(async () => {
  databaseName = `converge_compaction_candidates_${process.pid}_${crypto
    .randomUUID()
    .replaceAll("-", "")
    .slice(0, 10)}`;
  const adminUrl = new URL(sharedDatabaseUrl);
  adminUrl.pathname = "/postgres";
  adminPool = createPool(adminUrl.toString());
  await adminPool.query(`CREATE DATABASE "${databaseName}"`);
  const isolatedUrl = new URL(sharedDatabaseUrl);
  isolatedUrl.pathname = `/${databaseName}`;
  await runFile("pnpm", ["--filter", "@converge/database", "migrate"], {
    cwd: repositoryRoot,
    env: { ...process.env, DATABASE_URL: isolatedUrl.toString() },
    maxBuffer: 1024 * 1024,
  });
  pool = createPool(isolatedUrl.toString());
  boards = new BoardRepository(pool);
  snapshots = new BoardSnapshotRepository(pool);
  discovery = new BoardCompactionCandidateRepository(pool);
  compaction = new BoardCompactionRepository(pool);
  const migration = await pool.query<{ name: string }>(
    "SELECT name FROM converge_migrations ORDER BY name DESC LIMIT 1",
  );
  expect(migration.rows[0]?.name).toBe("0009_board_replay_receipts_and_recovery_floors.sql");
});

afterEach(async () => {
  if (boardIds.size > 0)
    await pool.query("DELETE FROM boards WHERE id = ANY($1::uuid[])", [[...boardIds]]);
  boardIds.clear();
});

afterAll(async () => {
  try {
    await pool?.end();
  } finally {
    try {
      if (adminPool && databaseName)
        await adminPool.query(`DROP DATABASE IF EXISTS "${databaseName}"`);
    } finally {
      await adminPool?.end();
    }
  }
});

describe("bounded compaction candidate discovery", () => {
  it("returns no candidate for zero or one verified generation", async () => {
    await createBoard("zero-generations");
    await createGenerations(1, "one-generation");

    await expect(discovery.discover(options())).resolves.toMatchObject({ candidates: [] });
  });

  it("selects the older of two verified generations", async () => {
    const context = await createGenerations(2, "two-generations");
    const result = await discovery.discover(options());

    expect(result.candidates).toEqual([
      {
        boardId: context.boardId,
        operationRecoveryFloor: 0,
        deliveryRecoveryFloor: 0,
        snapshotId: context.generations[0]?.id,
        snapshotCanvasSeq: 1,
        snapshotDeliverySeq: 1,
        canvasHead: 2,
        deliveryHead: 2,
      },
    ]);
  });

  it("selects exactly one verified generation behind the newest of three", async () => {
    const context = await createGenerations(3, "three-generations");
    const result = await discovery.discover(options());

    expect(result.candidates).toEqual([
      expect.objectContaining({
        boardId: context.boardId,
        snapshotId: context.generations[1]?.id,
        snapshotCanvasSeq: 2,
        snapshotDeliverySeq: 2,
        canvasHead: 3,
        deliveryHead: 3,
      }),
    ]);
  });

  it("ignores invalid and unverified snapshot generations", async () => {
    const context = await createGenerations(4, "ignored-generations");
    const creating = context.generations[2];
    const invalid = context.generations[3];
    if (!creating || !invalid) throw new Error("Missing snapshot fixture");
    await pool.query("ALTER TABLE board_snapshots DISABLE TRIGGER board_snapshots_immutable");
    try {
      await pool.query(
        "UPDATE board_snapshots SET status = 'creating', verified_at = NULL WHERE id = $1",
        [creating.id],
      );
    } finally {
      await pool.query("ALTER TABLE board_snapshots ENABLE TRIGGER board_snapshots_immutable");
    }
    await pool.query(
      `UPDATE board_snapshots
       SET status = 'invalid', invalidation_code = 'TEST_INVALID',
           invalidated_at = clock_timestamp()
       WHERE id = $1`,
      [invalid.id],
    );

    expect((await discovery.discover(options())).candidates).toEqual([
      expect.objectContaining({
        boardId: context.boardId,
        snapshotId: context.generations[0]?.id,
        snapshotCanvasSeq: 1,
      }),
    ]);
  });

  it("requires both coupled floors to lag the proposed boundary", async () => {
    const caughtUp = await createGenerations(2, "floors-caught-up");
    await pool.query(
      `UPDATE boards SET operation_recovery_floor = 1, delivery_recovery_floor = 1
       WHERE id = $1`,
      [caughtUp.boardId],
    );
    const operationCaughtUp = await createGenerations(2, "operation-floor-caught-up");
    await pool.query("UPDATE boards SET operation_recovery_floor = 1 WHERE id = $1", [
      operationCaughtUp.boardId,
    ]);
    const deliveryCaughtUp = await createGenerations(2, "delivery-floor-caught-up");
    await pool.query("UPDATE boards SET delivery_recovery_floor = 1 WHERE id = $1", [
      deliveryCaughtUp.boardId,
    ]);
    const bothLag = await createGenerations(2, "both-floors-lag");

    expect((await discovery.discover(options())).candidates.map(({ boardId }) => boardId)).toEqual([
      bothLag.boardId,
    ]);
  });

  it("enforces scan and result limits while advancing past every inspected board", async () => {
    const contexts = await Promise.all(
      Array.from({ length: 5 }, (_, index) => createGenerations(2, `limits-${index}`)),
    );
    const ordered = contexts.map(({ boardId }) => boardId).sort();
    const result = await discovery.discover(options(null, 4, 2));

    expect(result.inspectedCount).toBe(4);
    expect(result.candidates.map(({ boardId }) => boardId)).toEqual(ordered.slice(0, 2));
    expect(result.nextCursor).toBe(ordered[3]);
  });

  it("wraps once without duplicates and progresses fairly across calls", async () => {
    const contexts = await Promise.all(
      Array.from({ length: 4 }, (_, index) => createGenerations(2, `round-robin-${index}`)),
    );
    const ordered = contexts.map(({ boardId }) => boardId).sort();
    const cursor = ordered[1];
    if (!cursor) throw new Error("Missing cursor fixture");
    const wrapped = await discovery.discover(options(cursor, 3, 3));
    expect(wrapped.candidates.map(({ boardId }) => boardId)).toEqual([
      ordered[2],
      ordered[3],
      ordered[0],
    ]);
    expect(new Set(wrapped.candidates.map(({ boardId }) => boardId))).toHaveLength(3);
    expect(wrapped.nextCursor).toBe(ordered[0]);

    const first = await discovery.discover(options(null, 2, 2));
    const second = await discovery.discover(options(first.nextCursor, 2, 2));
    expect([
      ...first.candidates.map(({ boardId }) => boardId),
      ...second.candidates.map(({ boardId }) => boardId),
    ]).toEqual(ordered);
  });

  it("is read-only and never materializes payload tables or columns", async () => {
    const context = await createGenerations(2, "read-only");
    const before = await durableSummary(context.boardId);
    await discovery.discover(options());
    expect(await durableSummary(context.boardId)).toEqual(before);

    let queryText = "";
    const inspecting = new BoardCompactionCandidateRepository({
      query: (text: string) => {
        queryText = text;
        return Promise.resolve({ rows: [], rowCount: 0 });
      },
    } as unknown as pg.Pool);
    await inspecting.discover(options());
    expect(queryText).not.toMatch(
      /board_operations|board_operation_receipts|outbox_events|board_objects|projection|payload|command/i,
    );
  });

  it("strictly rejects invalid options and malformed or impossible evidence", async () => {
    await expect(discovery.discover(options("not-a-uuid"))).rejects.toMatchObject({
      code: "INVALID_CURSOR",
    });
    for (const invalid of [0, -1, 1.5, Number.MAX_SAFE_INTEGER])
      await expect(discovery.discover(options(null, invalid, 1))).rejects.toMatchObject({
        code: "INVALID_CONFIGURATION",
      });
    await expect(
      discovery.discover(options(null, 1, COMPACTION_CANDIDATE_RESULT_LIMIT_MAXIMUM + 1)),
    ).rejects.toMatchObject({ code: "INVALID_CONFIGURATION" });
    expect(COMPACTION_CANDIDATE_SCAN_LIMIT_MAXIMUM).toBe(100);

    const malformed = new BoardCompactionCandidateRepository({
      query: () =>
        Promise.resolve({
          rowCount: 1,
          rows: [
            {
              board_id: crypto.randomUUID(),
              last_seq: "1",
              last_delivery_seq: "1",
              operation_recovery_floor: "2",
              delivery_recovery_floor: "0",
              traversal_group: 0,
              newest_snapshot_id: crypto.randomUUID(),
              newest_snapshot_seq: "2",
              newest_snapshot_delivery_seq: "2",
              proposed_snapshot_id: crypto.randomUUID(),
              proposed_snapshot_seq: "1",
              proposed_snapshot_delivery_seq: "1",
            },
          ],
        }),
    } as unknown as pg.Pool);
    await expect(malformed.discover(options())).rejects.toBeInstanceOf(
      BoardCompactionCandidateError,
    );
  });

  it("throws unexpected PostgreSQL failures unchanged", async () => {
    const failure = Object.assign(new Error("injected PostgreSQL failure"), { code: "57P01" });
    const failing = new BoardCompactionCandidateRepository({
      query: vi.fn(() => Promise.reject(failure)),
    } as unknown as pg.Pool);

    await expect(failing.discover(options())).rejects.toBe(failure);
  });

  it("permits duplicate advisory discovery and leaves final authority to compaction", async () => {
    const context = await createGenerations(2, "advisory-authority", true);
    const peer = new BoardCompactionCandidateRepository(pool);
    const [first, second] = await Promise.all([
      discovery.discover(options()),
      peer.discover(options()),
    ]);
    expect(first.candidates).toEqual(second.candidates);
    expect(first.candidates).toHaveLength(1);

    await boards.commitOperation(fixtureIds.user, {
      ...createRectangleCommand(context.boardId),
      baseSeq: 2,
    });
    await expect(compaction.compact(context.boardId)).resolves.toMatchObject({
      outcome: "compacted",
      snapshotId: context.generations[0]?.id,
      newOperationFloor: 1,
      newDeliveryFloor: 1,
    });
    await expect(compaction.compact(context.boardId)).resolves.toMatchObject({
      outcome: "no_progress",
      snapshotId: context.generations[0]?.id,
    });
  });
});
