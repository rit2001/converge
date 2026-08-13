import type pg from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
  BoardCompactionRepository,
  BoardRecoveryMaterialRepository,
  BoardRepository,
  BoardSnapshotRepository,
  createPool,
} from "@converge/database";
import { createRectangleCommand, fixtureIds } from "@converge/testkit";
import type { DurableCommand } from "@converge/protocol";

const databaseUrl =
  process.env.DATABASE_URL ?? "postgresql://converge:converge@localhost:55432/converge";
const pool = createPool(databaseUrl);
const boards = new BoardRepository(pool);
const snapshots = new BoardSnapshotRepository(pool);
const compaction = new BoardCompactionRepository(pool);

interface DurableSummary {
  last_seq: string;
  last_delivery_seq: string;
  operation_recovery_floor: string;
  delivery_recovery_floor: string;
  operation_count: string;
  outbox_count: string;
  receipt_count: string;
  snapshot_count: string;
  membership_count: string;
}

interface PreservedChecksums {
  projection: string;
  memberships: string;
  snapshots: string;
  receipts: string;
}

async function createBoard(): Promise<string> {
  return (await boards.createBoard(fixtureIds.user, `compaction-${crypto.randomUUID()}`)).id;
}

async function publish(eventId: string, publicationId = `test-${crypto.randomUUID()}`) {
  await pool.query(
    `UPDATE outbox_events
     SET status = 'published', next_attempt_at = 'infinity'::timestamptz,
         redis_entry_id = $2, published_at = clock_timestamp(), updated_at = clock_timestamp()
     WHERE id = $1`,
    [eventId, publicationId],
  );
}

async function commit(boardId: string, baseSeq = 0) {
  const command: DurableCommand = { ...createRectangleCommand(boardId), baseSeq };
  const result = await boards.commitOperation(fixtureIds.user, command);
  return { command, ...result };
}

async function twoSnapshotBoard() {
  const boardId = await createBoard();
  const first = await commit(boardId);
  await publish(first.event.eventId);
  const firstSnapshot = await snapshots.create(boardId);
  const second = await commit(boardId, 1);
  await publish(second.event.eventId);
  const secondSnapshot = await snapshots.create(boardId);
  return { boardId, first, second, firstSnapshot, secondSnapshot };
}

async function durableSummary(boardId: string): Promise<DurableSummary> {
  const result = await pool.query<DurableSummary>(
    `SELECT b.last_seq, b.last_delivery_seq,
            b.operation_recovery_floor, b.delivery_recovery_floor,
            (SELECT count(*) FROM board_operations WHERE board_id = b.id) operation_count,
            (SELECT count(*) FROM outbox_events WHERE board_id = b.id) outbox_count,
            (SELECT count(*) FROM board_operation_receipts WHERE board_id = b.id) receipt_count,
            (SELECT count(*) FROM board_snapshots WHERE board_id = b.id) snapshot_count,
            (SELECT count(*) FROM board_members WHERE board_id = b.id) membership_count
     FROM boards b WHERE b.id = $1`,
    [boardId],
  );
  const row = result.rows[0];
  if (!row) throw new Error("Board summary is unavailable");
  return row;
}

async function preservedChecksums(boardId: string): Promise<PreservedChecksums> {
  const result = await pool.query<PreservedChecksums>(
    `SELECT
       (SELECT md5(COALESCE(string_agg(to_jsonb(object_row)::text, ',' ORDER BY object_id), ''))
        FROM (SELECT * FROM board_objects WHERE board_id = $1) object_row) projection,
       (SELECT md5(COALESCE(string_agg(to_jsonb(member_row)::text, ',' ORDER BY user_id), ''))
        FROM (SELECT * FROM board_members WHERE board_id = $1) member_row) memberships,
       (SELECT md5(COALESCE(string_agg(to_jsonb(snapshot_row)::text, ',' ORDER BY id), ''))
        FROM (SELECT * FROM board_snapshots WHERE board_id = $1) snapshot_row) snapshots,
       (SELECT md5(COALESCE(string_agg(to_jsonb(receipt_row)::text, ',' ORDER BY operation_id), ''))
        FROM (SELECT * FROM board_operation_receipts WHERE board_id = $1) receipt_row) receipts`,
    [boardId],
  );
  const row = result.rows[0];
  if (!row) throw new Error("Preserved-state checksums are unavailable");
  return row;
}

function deferred(): { promise: Promise<void>; resolve: () => void } {
  let resolve = (): void => undefined;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

beforeAll(async () => {
  await pool.query("SELECT 1");
  const migration = await pool.query<{ name: string }>(
    "SELECT name FROM converge_migrations ORDER BY name DESC LIMIT 1",
  );
  expect(migration.rows[0]?.name).toBe("0009_board_replay_receipts_and_recovery_floors.sql");
});

afterAll(async () => {
  await pool.end();
});

describe("transactional board compaction", () => {
  it("makes no mutation without a safety-delayed verified boundary", async () => {
    const boardId = await createBoard();
    const operation = await commit(boardId);
    await publish(operation.event.eventId);
    await snapshots.create(boardId);
    const before = await durableSummary(boardId);

    await expect(compaction.compact(boardId)).resolves.toEqual({
      outcome: "no_verified_boundary",
      boardId,
    });
    expect(await durableSummary(boardId)).toEqual(before);
  });

  it("compacts only the older verified snapshot boundary and retains every receipt", async () => {
    const context = await twoSnapshotBoard();
    const beforeChecksums = await preservedChecksums(context.boardId);
    const result = await compaction.compact(context.boardId);

    expect(result).toMatchObject({
      outcome: "compacted",
      boardId: context.boardId,
      previousOperationFloor: 0,
      newOperationFloor: 1,
      previousDeliveryFloor: 0,
      newDeliveryFloor: 1,
      deletedOperationCount: 1,
      deletedOutboxCount: 1,
      snapshotId: context.firstSnapshot.id,
    });
    expect(await durableSummary(context.boardId)).toMatchObject({
      last_seq: "2",
      last_delivery_seq: "2",
      operation_recovery_floor: "1",
      delivery_recovery_floor: "1",
      operation_count: "1",
      outbox_count: "1",
      receipt_count: "2",
      snapshot_count: "2",
      membership_count: "1",
    });
    expect(await preservedChecksums(context.boardId)).toEqual(beforeChecksums);
    expect(
      (
        await pool.query<{ seq: string }>(
          "SELECT seq FROM board_operations WHERE board_id = $1 ORDER BY seq",
          [context.boardId],
        )
      ).rows,
    ).toEqual([{ seq: "2" }]);
  });

  it("returns the original acknowledgement from the retained receipt after deletion", async () => {
    const context = await twoSnapshotBoard();
    await compaction.compact(context.boardId);
    const beforeReplay = await durableSummary(context.boardId);

    await expect(boards.commitOperation(fixtureIds.user, context.first.command)).resolves.toEqual({
      duplicate: true,
      operation: context.first.operation,
      event: context.first.event,
    });
    expect(await durableSummary(context.boardId)).toEqual(beforeReplay);
  });

  it("blocks missing and mismatched receipt evidence without deleting operations", async () => {
    const missing = await twoSnapshotBoard();
    await pool.query(
      "DELETE FROM board_operation_receipts WHERE board_id = $1 AND operation_id = $2",
      [missing.boardId, missing.first.command.opId],
    );
    const missingBefore = await durableSummary(missing.boardId);
    await expect(compaction.compact(missing.boardId)).resolves.toMatchObject({
      outcome: "blocked",
      code: "OPERATION_RECEIPT_EVIDENCE_INVALID",
    });
    expect(await durableSummary(missing.boardId)).toEqual(missingBefore);

    const mismatched = await twoSnapshotBoard();
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      await client.query(
        "ALTER TABLE board_operation_receipts DISABLE TRIGGER board_operation_receipts_immutable",
      );
      await client.query(
        "UPDATE board_operation_receipts SET actor_id = $3 WHERE board_id = $1 AND operation_id = $2",
        [mismatched.boardId, mismatched.first.command.opId, crypto.randomUUID()],
      );
      await client.query(
        "ALTER TABLE board_operation_receipts ENABLE TRIGGER board_operation_receipts_immutable",
      );
      await client.query("COMMIT");
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
    const mismatchBefore = await durableSummary(mismatched.boardId);
    await expect(compaction.compact(mismatched.boardId)).resolves.toMatchObject({
      outcome: "blocked",
      code: "OPERATION_RECEIPT_EVIDENCE_INVALID",
    });
    expect(await durableSummary(mismatched.boardId)).toEqual(mismatchBefore);
  });

  it.each([
    ["pending", "OUTBOX_PUBLICATION_EVIDENCE_INVALID"],
    ["leased", "OUTBOX_PUBLICATION_EVIDENCE_INVALID"],
    ["retry_wait", "OUTBOX_PUBLICATION_EVIDENCE_INVALID"],
    ["blocked", "OUTBOX_PUBLICATION_EVIDENCE_INVALID"],
    ["malformed", "OUTBOX_PUBLICATION_EVIDENCE_INVALID"],
    ["inconsistent", "OUTBOX_PUBLICATION_EVIDENCE_INVALID"],
    ["unpublished", "OUTBOX_PUBLICATION_EVIDENCE_INVALID"],
    ["missing", "OPERATION_RECEIPT_EVIDENCE_INVALID"],
  ])("does not pass %s outbox evidence", async (unsafeState, expectedCode) => {
    const context = await twoSnapshotBoard();
    if (unsafeState === "pending" || unsafeState === "unpublished") {
      await pool.query(
        `UPDATE outbox_events
           SET status = 'pending', next_attempt_at = clock_timestamp(),
               redis_entry_id = NULL, published_at = NULL
           WHERE id = $1`,
        [context.first.event.eventId],
      );
    } else if (unsafeState === "leased") {
      await pool.query(
        `UPDATE outbox_events
           SET status = 'leased', attempt_count = 1, lease_owner = 'compaction-test',
               lease_token = $2, leased_until = clock_timestamp() + interval '1 hour',
               next_attempt_at = clock_timestamp(), redis_entry_id = NULL, published_at = NULL
           WHERE id = $1`,
        [context.first.event.eventId, crypto.randomUUID()],
      );
    } else if (unsafeState === "retry_wait") {
      await pool.query(
        `UPDATE outbox_events
           SET status = 'retry_wait', attempt_count = 1, next_attempt_at = clock_timestamp(),
               redis_entry_id = NULL, published_at = NULL,
               last_error_code = 'TEST', last_error_message = 'retry',
               last_error_at = clock_timestamp()
           WHERE id = $1`,
        [context.first.event.eventId],
      );
    } else if (unsafeState === "blocked") {
      await pool.query(
        `UPDATE outbox_events
           SET status = 'blocked', attempt_count = 1,
               next_attempt_at = 'infinity'::timestamptz,
               redis_entry_id = NULL, published_at = NULL,
               last_error_code = 'TEST', last_error_message = 'blocked',
               last_error_at = clock_timestamp()
           WHERE id = $1`,
        [context.first.event.eventId],
      );
    } else if (unsafeState === "malformed") {
      await pool.query(
        "UPDATE outbox_events SET redis_entry_id = E'malformed\\nentry' WHERE id = $1",
        [context.first.event.eventId],
      );
    } else if (unsafeState === "inconsistent") {
      await pool.query("UPDATE outbox_events SET published_at = 'infinity' WHERE id = $1", [
        context.first.event.eventId,
      ]);
    } else {
      await pool.query("DELETE FROM outbox_events WHERE id = $1", [context.first.event.eventId]);
    }
    const before = await durableSummary(context.boardId);
    await expect(compaction.compact(context.boardId)).resolves.toMatchObject({
      outcome: "blocked",
      code: expectedCode,
    });
    expect(await durableSummary(context.boardId)).toEqual(before);
  });

  it("keeps floors coupled, bounded by snapshot/board heads, and is idempotent", async () => {
    const context = await twoSnapshotBoard();
    const first = await compaction.compact(context.boardId);
    expect(first).toMatchObject({
      outcome: "compacted",
      newOperationFloor: 1,
      newDeliveryFloor: 1,
    });
    await expect(compaction.compact(context.boardId)).resolves.toMatchObject({
      outcome: "no_progress",
      previousOperationFloor: 1,
      newOperationFloor: 1,
      previousDeliveryFloor: 1,
      newDeliveryFloor: 1,
      deletedOperationCount: 0,
      deletedOutboxCount: 0,
    });
    expect(await durableSummary(context.boardId)).toMatchObject({
      last_seq: "2",
      last_delivery_seq: "2",
      operation_recovery_floor: "1",
      delivery_recovery_floor: "1",
    });
  });

  it("uses a newer verified generation for later forward progress", async () => {
    const context = await twoSnapshotBoard();
    await compaction.compact(context.boardId);
    const third = await commit(context.boardId, 2);
    await publish(third.event.eventId);
    await snapshots.create(context.boardId);

    await expect(compaction.compact(context.boardId)).resolves.toMatchObject({
      outcome: "compacted",
      previousOperationFloor: 1,
      newOperationFloor: 2,
      previousDeliveryFloor: 1,
      newDeliveryFloor: 2,
      snapshotId: context.secondSnapshot.id,
    });
  });

  it("does not use an invalid newest snapshot as a boundary", async () => {
    const context = await twoSnapshotBoard();
    const third = await commit(context.boardId, 2);
    await publish(third.event.eventId);
    const invalid = await snapshots.create(context.boardId);
    await pool.query(
      `UPDATE board_snapshots
       SET status = 'invalid', invalidation_code = 'TEST_INVALID',
           invalidated_at = clock_timestamp()
       WHERE id = $1`,
      [invalid.id],
    );

    await expect(compaction.compact(context.boardId)).resolves.toMatchObject({
      outcome: "compacted",
      snapshotId: context.firstSnapshot.id,
      newOperationFloor: 1,
    });
  });

  it("does not use an unverified snapshot as a boundary", async () => {
    const context = await twoSnapshotBoard();
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      await client.query("ALTER TABLE board_snapshots DISABLE TRIGGER board_snapshots_immutable");
      await client.query(
        `UPDATE board_snapshots
         SET status = 'creating', verified_at = NULL
         WHERE id = $1`,
        [context.secondSnapshot.id],
      );
      await client.query("ALTER TABLE board_snapshots ENABLE TRIGGER board_snapshots_immutable");
      await client.query("COMMIT");
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
    const before = await durableSummary(context.boardId);

    await expect(compaction.compact(context.boardId)).resolves.toEqual({
      outcome: "no_verified_boundary",
      boardId: context.boardId,
    });
    expect(await durableSummary(context.boardId)).toEqual(before);
  });

  it("serializes a foreground operation wholly after compaction", async () => {
    const context = await twoSnapshotBoard();
    const locked = deferred();
    const release = deferred();
    const writerAttempting = deferred();
    const compacting = new BoardCompactionRepository(pool, {
      afterAdvisoryLock: async () => {
        locked.resolve();
        await release.promise;
      },
    });
    const writing = new BoardRepository(pool, {
      beforeSequenceLock: () => {
        writerAttempting.resolve();
        return Promise.resolve();
      },
    });
    const compactionPromise = compacting.compact(context.boardId);
    await locked.promise;
    const writerPromise = writing.commitOperation(fixtureIds.user, {
      ...createRectangleCommand(context.boardId),
      baseSeq: 2,
    });
    await writerAttempting.promise;
    release.resolve();

    await expect(compactionPromise).resolves.toMatchObject({ outcome: "compacted" });
    await expect(writerPromise).resolves.toMatchObject({ operation: { seq: 3 } });
    expect(await durableSummary(context.boardId)).toMatchObject({
      last_seq: "3",
      operation_recovery_floor: "1",
      operation_count: "2",
    });
  });

  it("serializes a membership revocation wholly after compaction", async () => {
    const context = await twoSnapshotBoard();
    const targetUserId = crypto.randomUUID();
    await pool.query(
      "INSERT INTO board_members(board_id, user_id, role) VALUES ($1, $2, 'editor')",
      [context.boardId, targetUserId],
    );
    const locked = deferred();
    const release = deferred();
    const revocationAttempting = deferred();
    const compacting = new BoardCompactionRepository(pool, {
      afterAdvisoryLock: async () => {
        locked.resolve();
        await release.promise;
      },
    });
    const revoking = new BoardRepository(pool, {
      beforeMembershipSequenceLock: () => {
        revocationAttempting.resolve();
        return Promise.resolve();
      },
    });
    const compactionPromise = compacting.compact(context.boardId);
    await locked.promise;
    const revocationPromise = revoking.removeBoardMember(
      fixtureIds.user,
      context.boardId,
      targetUserId,
    );
    await revocationAttempting.promise;
    release.resolve();

    await expect(compactionPromise).resolves.toMatchObject({ outcome: "compacted" });
    await expect(revocationPromise).resolves.toMatchObject({
      removed: true,
      event: { deliverySeq: 3 },
    });
    expect(await durableSummary(context.boardId)).toMatchObject({
      last_delivery_seq: "3",
      delivery_recovery_floor: "1",
      outbox_count: "2",
      membership_count: "1",
    });
  });

  it("rolls back deletions and floors on a forced pre-floor failure", async () => {
    const context = await twoSnapshotBoard();
    const before = await durableSummary(context.boardId);
    const failing = new BoardCompactionRepository(pool, {
      afterDeletionsBeforeFloorUpdate: () => Promise.reject(new Error("forced compaction failure")),
    });
    await expect(failing.compact(context.boardId)).rejects.toThrow("forced compaction failure");
    expect(await durableSummary(context.boardId)).toEqual(before);
  });

  it("throws unexpected database failures instead of returning blocked", async () => {
    const unavailable = new Error("database unavailable");
    const failingPool = {
      connect: () => Promise.reject(unavailable),
    } as unknown as pg.Pool;
    await expect(
      new BoardCompactionRepository(failingPool).compact(crypto.randomUUID()),
    ).rejects.toBe(unavailable);
  });

  it("retains trustworthy snapshot-plus-tail recovery after compaction", async () => {
    const context = await twoSnapshotBoard();
    const third = await commit(context.boardId, 2);
    await publish(third.event.eventId);
    await compaction.compact(context.boardId);

    const material = await new BoardRecoveryMaterialRepository(pool).load(context.boardId);
    expect(material.snapshotId).toBe(context.secondSnapshot.id);
    expect(material.operations.map(({ seq }) => seq)).toEqual([3]);
    expect(material.capturedCanvasSeq).toBe(3);
    expect(material.reconstructedState.lastSeq).toBe(3);
  });
});
