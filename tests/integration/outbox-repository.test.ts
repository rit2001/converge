import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import type { PoolClient } from "pg";
import {
  BoardRepository,
  calculateOutboxRetryDelayMs,
  createPool,
  MAX_OUTBOX_CLAIM_BATCH_SIZE,
  MAX_OUTBOX_LEASE_MS,
  OutboxRepository,
  OUTBOX_MAX_ATTEMPTS,
} from "@converge/database";
import {
  deliveryEnvelopeSchema,
  membershipRevokedDeliveryEnvelopeSchema,
} from "@converge/protocol";
import { createRectangleCommand, fixtureIds } from "@converge/testkit";

const databaseUrl =
  process.env.DATABASE_URL ?? "postgresql://converge:converge@localhost:55432/converge";
const pool = createPool(databaseUrl);
const boardRepository = new BoardRepository(pool);
const outboxRepository = new OutboxRepository(pool);
const createdBoardIds = new Set<string>();

interface SeededEvent {
  eventId: string;
  boardId: string;
  deliverySeq: number;
}

interface OutboxState {
  id: string;
  board_id: string;
  delivery_seq: string;
  status: string;
  attempt_count: number;
  lease_owner: string | null;
  lease_token: string | null;
  leased_until: Date | null;
  next_attempt_at: Date;
  next_attempt_infinite: boolean;
  redis_entry_id: string | null;
  published_at: Date | null;
  last_error_code: string | null;
  last_error_message: string | null;
  last_error_at: Date | null;
  updated_at: Date;
  payload: unknown;
}

beforeAll(async () => {
  await pool.query("SELECT 1");
});

afterEach(async () => {
  if (createdBoardIds.size === 0) return;
  await pool.query("DELETE FROM boards WHERE id = ANY($1::uuid[])", [[...createdBoardIds]]);
  createdBoardIds.clear();
});

afterAll(async () => {
  await pool.end();
});

function deferred(): { promise: Promise<void>; resolve(): void } {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

async function seedRevocationBoard(eventCount = 1): Promise<SeededEvent[]> {
  const boardId = crypto.randomUUID();
  createdBoardIds.add(boardId);
  const events: SeededEvent[] = [];
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    await client.query(
      `INSERT INTO boards(id, name, created_by, last_delivery_seq)
       VALUES ($1, $2, $3, $4)`,
      [boardId, `outbox-${boardId}`, fixtureIds.user, eventCount],
    );
    for (let deliverySeq = 1; deliverySeq <= eventCount; deliverySeq += 1) {
      const eventId = crypto.randomUUID();
      const envelope = membershipRevokedDeliveryEnvelopeSchema.parse({
        schemaVersion: 1,
        eventId,
        boardId,
        deliverySeq,
        eventType: "board.membership.revoked",
        occurredAt: `2026-08-09T12:00:${String(deliverySeq).padStart(2, "0")}.000Z`,
        payload: {
          revokedUserId: crypto.randomUUID(),
          initiatedByUserId: fixtureIds.user,
        },
      });
      await client.query(
        `INSERT INTO outbox_events(
           id, board_id, delivery_seq, canvas_seq, event_type, schema_version, payload
         ) VALUES ($1,$2,$3,NULL,'board.membership.revoked',1,$4)`,
        [eventId, boardId, deliverySeq, envelope],
      );
      events.push({ eventId, boardId, deliverySeq });
    }
    await client.query("COMMIT");
    return events;
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

async function seedOperationBoard(): Promise<SeededEvent> {
  const board = await boardRepository.createBoard(
    fixtureIds.user,
    `outbox-op-${crypto.randomUUID()}`,
  );
  createdBoardIds.add(board.id);
  const committed = await boardRepository.commitOperation(
    fixtureIds.user,
    createRectangleCommand(board.id),
  );
  return { eventId: committed.event.eventId, boardId: board.id, deliverySeq: 1 };
}

async function stateFor(eventId: string): Promise<OutboxState> {
  const result = await pool.query<OutboxState>(
    `SELECT id, board_id, delivery_seq, status, attempt_count, lease_owner, lease_token,
            leased_until, next_attempt_at,
            next_attempt_at = 'infinity'::timestamptz next_attempt_infinite,
            redis_entry_id, published_at, last_error_code,
            last_error_message, last_error_at, updated_at, payload
     FROM outbox_events WHERE id = $1`,
    [eventId],
  );
  const row = result.rows[0];
  if (!row) throw new Error(`Missing outbox event ${eventId}`);
  return row;
}

async function expireLease(eventId: string): Promise<void> {
  await pool.query(
    `UPDATE outbox_events
     SET leased_until = statement_timestamp() - interval '1 second'
     WHERE id = $1 AND status = 'leased'`,
    [eventId],
  );
}

async function expectConstraintFailure(
  client: PoolClient,
  eventId: string,
  assignment: string,
  parameters: unknown[] = [],
): Promise<void> {
  await client.query("SAVEPOINT invalid_outbox_state");
  await expect(
    client.query(`UPDATE outbox_events SET ${assignment} WHERE id = $1`, [eventId, ...parameters]),
  ).rejects.toMatchObject({ code: "23514" });
  await client.query("ROLLBACK TO SAVEPOINT invalid_outbox_state");
  await client.query("RELEASE SAVEPOINT invalid_outbox_state");
}

describe("leased PostgreSQL outbox repository", () => {
  it("uses SKIP LOCKED so concurrent claimers never share an event or overtake its board", async () => {
    const [first, second] = await seedRevocationBoard(2);
    const [unrelated] = await seedRevocationBoard();
    if (!first || !second || !unrelated) throw new Error("Expected seeded events");
    await pool.query(
      `UPDATE outbox_events
       SET created_at = CASE id
         WHEN $1 THEN '2026-08-09T12:00:00Z'::timestamptz
         WHEN $2 THEN '2026-08-09T12:00:01Z'::timestamptz
         ELSE '2026-08-09T12:00:02Z'::timestamptz
       END
       WHERE id = ANY($3::uuid[])`,
      [first.eventId, second.eventId, [first.eventId, second.eventId, unrelated.eventId]],
    );
    const firstClaimUpdated = deferred();
    const releaseFirstClaim = deferred();
    const holdingRepository = new OutboxRepository(pool, {
      afterClaimUpdate: async () => {
        firstClaimUpdated.resolve();
        await releaseFirstClaim.promise;
      },
    });

    const firstClaimPromise = holdingRepository.claimAvailable({
      owner: "worker-a",
      batchSize: 1,
    });
    await firstClaimUpdated.promise;
    const concurrentClaims = await outboxRepository.claimAvailable({
      owner: "worker-b",
      batchSize: 32,
    });
    expect(concurrentClaims.map((claim) => claim.eventId)).toEqual([unrelated.eventId]);
    expect(concurrentClaims.some((claim) => claim.eventId === second.eventId)).toBe(false);
    releaseFirstClaim.resolve();
    const firstClaims = await firstClaimPromise;
    expect(firstClaims.map((claim) => claim.eventId)).toEqual([first.eventId]);
    expect(new Set([...firstClaims, ...concurrentClaims].map((claim) => claim.eventId)).size).toBe(
      2,
    );
  });

  it("skips an active claim and enforces the bounded default batch size", async () => {
    const boards = await Promise.all(
      Array.from({ length: MAX_OUTBOX_CLAIM_BATCH_SIZE + 8 }, () => seedRevocationBoard()),
    );
    const allEvents = boards.flat();
    const firstBatch = await outboxRepository.claimAvailable({ owner: "batch-worker" });
    expect(firstBatch).toHaveLength(MAX_OUTBOX_CLAIM_BATCH_SIZE);
    const secondBatch = await outboxRepository.claimAvailable({ owner: "batch-worker-2" });
    expect(secondBatch).toHaveLength(8);
    expect(new Set([...firstBatch, ...secondBatch].map((claim) => claim.eventId)).size).toBe(
      allEvents.length,
    );
    await expect(
      outboxRepository.claimAvailable({ owner: "batch-worker-3", batchSize: 33 }),
    ).rejects.toBeInstanceOf(RangeError);
    await expect(
      outboxRepository.claimAvailable({
        owner: "batch-worker-3",
        leaseDurationMs: MAX_OUTBOX_LEASE_MS + 1,
      }),
    ).rejects.toBeInstanceOf(RangeError);
    await expect(
      outboxRepository.claimAvailable({ owner: "invalid owner" }),
    ).rejects.toBeInstanceOf(TypeError);
  });

  it("publishes atomically, never reclaims published work, then releases the next board event", async () => {
    const [first, second] = await seedRevocationBoard(2);
    if (!first || !second) throw new Error("Expected seeded events");
    const [claim] = await outboxRepository.claimAvailable({ owner: "publisher", batchSize: 1 });
    if (!claim) throw new Error("Expected claim");
    expect(claim.eventId).toBe(first.eventId);
    expect(await outboxRepository.claimAvailable({ owner: "observer" })).toEqual([]);

    const published = await outboxRepository.markPublished({
      eventId: claim.eventId,
      leaseToken: claim.leaseToken,
      publicationId: "1000-0",
    });
    expect(published).toMatchObject({ outcome: "applied", status: "published" });
    expect(await stateFor(first.eventId)).toMatchObject({
      status: "published",
      lease_owner: null,
      lease_token: null,
      leased_until: null,
      redis_entry_id: "1000-0",
    });
    await expect(
      outboxRepository.markPublished({
        eventId: claim.eventId,
        leaseToken: claim.leaseToken,
        publicationId: "1000-0",
      }),
    ).resolves.toEqual({ outcome: "stale", eventId: claim.eventId });
    const nextClaims = await outboxRepository.claimAvailable({ owner: "publisher" });
    expect(nextClaims.map((next) => next.eventId)).toEqual([second.eventId]);
  });

  it("reclaims an abandoned expired lease with a new token and stable event identity", async () => {
    const [seeded, later] = await seedRevocationBoard(2);
    if (!seeded || !later) throw new Error("Expected seeded events");
    const [original] = await outboxRepository.claimAvailable({ owner: "crashed-worker" });
    if (!original) throw new Error("Expected original claim");
    await expireLease(seeded.eventId);
    const recoveryClaims = await outboxRepository.claimAvailable({ owner: "recovery-worker" });
    const [reclaimed] = recoveryClaims;
    if (!reclaimed) throw new Error("Expected reclaimed event");
    expect(recoveryClaims).toHaveLength(1);
    expect(recoveryClaims.some((claim) => claim.eventId === later.eventId)).toBe(false);
    expect(reclaimed).toMatchObject({
      eventId: original.eventId,
      boardId: original.boardId,
      deliverySeq: original.deliverySeq,
      envelope: original.envelope,
      attemptCount: 2,
    });
    expect(reclaimed.leaseToken).not.toBe(original.leaseToken);
  });

  it("fences stale tokens from publish, failure, and lease renewal", async () => {
    const [seeded] = await seedRevocationBoard();
    if (!seeded) throw new Error("Expected seeded event");
    const [original] = await outboxRepository.claimAvailable({ owner: "worker-old" });
    if (!original) throw new Error("Expected original claim");
    await expireLease(seeded.eventId);
    const [current] = await outboxRepository.claimAvailable({ owner: "worker-current" });
    if (!current) throw new Error("Expected current claim");
    const before = await stateFor(seeded.eventId);
    await expect(
      outboxRepository.markPublished({
        eventId: seeded.eventId,
        leaseToken: original.leaseToken,
        publicationId: "1001-0",
      }),
    ).resolves.toMatchObject({ outcome: "stale" });
    await expect(
      outboxRepository.recordFailure({
        eventId: seeded.eventId,
        leaseToken: original.leaseToken,
        retryable: true,
        retryJitter: 0.5,
        errorCode: "REDIS_DOWN",
        errorMessage: "stale failure",
      }),
    ).resolves.toMatchObject({ outcome: "stale" });
    await expect(
      outboxRepository.renewLease({
        eventId: seeded.eventId,
        leaseToken: original.leaseToken,
      }),
    ).resolves.toMatchObject({ outcome: "stale" });
    expect(await stateFor(seeded.eventId)).toEqual(before);
  });

  it("renews only an unexpired current lease", async () => {
    const [seeded] = await seedRevocationBoard();
    if (!seeded) throw new Error("Expected seeded event");
    const [claim] = await outboxRepository.claimAvailable({
      owner: "renew-worker",
      leaseDurationMs: 1_000,
    });
    if (!claim) throw new Error("Expected claim");
    const renewed = await outboxRepository.renewLease({
      eventId: seeded.eventId,
      leaseToken: claim.leaseToken,
      leaseDurationMs: 60_000,
    });
    expect(renewed).toMatchObject({ outcome: "applied", status: "leased" });
    if (renewed.outcome !== "applied" || !renewed.leasedUntil)
      throw new Error("Expected renewed lease");
    expect(renewed.leasedUntil.getTime()).toBeGreaterThan(claim.leasedUntil.getTime());
    await expireLease(seeded.eventId);
    await expect(
      outboxRepository.renewLease({
        eventId: seeded.eventId,
        leaseToken: claim.leaseToken,
      }),
    ).resolves.toMatchObject({ outcome: "stale" });
  });

  it("uses deterministic full-jitter retry deadlines and waits until eligibility", async () => {
    expect(calculateOutboxRetryDelayMs(1, 0)).toBe(0);
    expect(calculateOutboxRetryDelayMs(1, 0.5)).toBe(125);
    expect(calculateOutboxRetryDelayMs(20, 0.999_999)).toBeLessThanOrEqual(30_000);
    const [seeded] = await seedRevocationBoard();
    if (!seeded) throw new Error("Expected seeded event");
    const [claim] = await outboxRepository.claimAvailable({ owner: "retry-worker" });
    if (!claim) throw new Error("Expected claim");
    const failed = await outboxRepository.recordFailure({
      eventId: seeded.eventId,
      leaseToken: claim.leaseToken,
      retryable: true,
      retryJitter: 0.5,
      errorCode: " redis down\nwith details ",
      errorMessage: ` secret\n${"x".repeat(600)} `,
    });
    expect(failed).toMatchObject({ outcome: "applied", status: "retry_wait" });
    const failedState = await stateFor(seeded.eventId);
    expect(failedState.last_error_code).toBe("redis_down_with_details");
    expect(failedState.last_error_message?.length).toBeLessThanOrEqual(500);
    if (!failedState.last_error_at) throw new Error("Expected failure timestamp");
    expect(failedState.next_attempt_at.getTime() - failedState.last_error_at.getTime()).toBe(125);

    await pool.query(
      `UPDATE outbox_events
       SET next_attempt_at = statement_timestamp() + interval '1 hour'
       WHERE id = $1`,
      [seeded.eventId],
    );
    expect(await outboxRepository.claimAvailable({ owner: "too-early" })).toEqual([]);
    await pool.query(
      `UPDATE outbox_events
       SET next_attempt_at = statement_timestamp() - interval '1 second'
       WHERE id = $1`,
      [seeded.eventId],
    );
    const [retried] = await outboxRepository.claimAvailable({ owner: "retry-worker-2" });
    expect(retried).toMatchObject({ eventId: seeded.eventId, attemptCount: 2 });
  });

  it("operator-blocks at attempt 20, blocks its board, and permits operator retry", async () => {
    const [head, later] = await seedRevocationBoard(2);
    const [unrelated] = await seedRevocationBoard();
    if (!head || !later || !unrelated) throw new Error("Expected seeded events");
    await pool.query("UPDATE outbox_events SET attempt_count = 19 WHERE id = $1", [head.eventId]);
    const [claim] = await outboxRepository.claimAvailable({ owner: "last-attempt", batchSize: 1 });
    if (!claim) throw new Error("Expected claim");
    expect(claim).toMatchObject({ eventId: head.eventId, attemptCount: OUTBOX_MAX_ATTEMPTS });
    const blocked = await outboxRepository.recordFailure({
      eventId: head.eventId,
      leaseToken: claim.leaseToken,
      retryable: true,
      errorCode: "REDIS_UNAVAILABLE",
      errorMessage: "attempt ceiling reached",
    });
    expect(blocked).toMatchObject({ outcome: "applied", status: "blocked" });
    expect((await stateFor(head.eventId)).next_attempt_infinite).toBe(true);
    const progress = await outboxRepository.claimAvailable({ owner: "other-board" });
    expect(progress.map((event) => event.eventId)).toEqual([unrelated.eventId]);
    expect(progress.some((event) => event.eventId === later.eventId)).toBe(false);

    await expect(outboxRepository.retryBlocked(head.eventId)).resolves.toEqual({
      outcome: "applied",
      eventId: head.eventId,
      status: "pending",
      attemptCount: 0,
    });
    const [retried] = await outboxRepository.claimAvailable({ owner: "operator-retry" });
    expect(retried).toMatchObject({ eventId: head.eventId, attemptCount: 1 });
  });

  it("lets unrelated boards progress while heads are leased, delayed, or blocked", async () => {
    const states = ["leased", "retry_wait", "blocked"] as const;
    for (const state of states) {
      const [head] = await seedRevocationBoard(2);
      const [unrelated] = await seedRevocationBoard();
      if (!head || !unrelated) throw new Error("Expected seeded events");
      const [claim] = await outboxRepository.claimAvailable({
        owner: `${state}-owner`,
        batchSize: 1,
      });
      if (!claim) throw new Error("Expected head claim");
      if (state === "retry_wait") {
        await outboxRepository.recordFailure({
          eventId: claim.eventId,
          leaseToken: claim.leaseToken,
          retryable: true,
          retryJitter: 0.5,
          errorCode: "RETRY",
          errorMessage: "retry later",
        });
        await pool.query(
          `UPDATE outbox_events
           SET next_attempt_at = statement_timestamp() + interval '1 hour'
           WHERE id = $1`,
          [claim.eventId],
        );
      } else if (state === "blocked") {
        await outboxRepository.recordFailure({
          eventId: claim.eventId,
          leaseToken: claim.leaseToken,
          retryable: false,
          errorCode: "INVALID_ENVELOPE",
          errorMessage: "operator action required",
        });
      }
      const claims = await outboxRepository.claimAvailable({ owner: `${state}-progress` });
      expect(claims.map((event) => event.eventId)).toContain(unrelated.eventId);
      expect(claims.some((event) => event.boardId === head.boardId)).toBe(false);
    }
  });

  it("ignores historical published rows and returns strictly validated envelopes", async () => {
    const operation = await seedOperationBoard();
    const [revocation] = await seedRevocationBoard();
    const [historical] = await seedRevocationBoard();
    if (!revocation || !historical) throw new Error("Expected seeded events");
    await pool.query(
      `UPDATE outbox_events
       SET status = 'published',
           next_attempt_at = 'infinity'::timestamptz,
           redis_entry_id = 'legacy_backfill',
           published_at = statement_timestamp(),
           updated_at = statement_timestamp()
       WHERE id = $1`,
      [historical.eventId],
    );
    const claims = await outboxRepository.claimAvailable({ owner: "schema-worker" });
    expect(new Set(claims.map((claim) => claim.eventId))).toEqual(
      new Set([operation.eventId, revocation.eventId]),
    );
    expect(claims.some((claim) => claim.eventId === historical.eventId)).toBe(false);
    for (const claim of claims)
      expect(deliveryEnvelopeSchema.parse(claim.envelope)).toEqual(claim.envelope);
  });

  it("rejects impossible lifecycle states with database constraints", async () => {
    const [seeded] = await seedRevocationBoard();
    if (!seeded) throw new Error("Expected seeded event");
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      await expectConstraintFailure(
        client,
        seeded.eventId,
        "status = 'leased', attempt_count = 1, lease_owner = 'partial', lease_token = $2",
        [crypto.randomUUID()],
      );
      await expectConstraintFailure(client, seeded.eventId, "lease_token = $2", [
        crypto.randomUUID(),
      ]);
      await expectConstraintFailure(
        client,
        seeded.eventId,
        "status = 'published', next_attempt_at = 'infinity'::timestamptz, published_at = statement_timestamp()",
      );
      await expectConstraintFailure(client, seeded.eventId, "redis_entry_id = '1002-0'");
      await expectConstraintFailure(
        client,
        seeded.eventId,
        "status = 'retry_wait', attempt_count = 1, next_attempt_at = statement_timestamp()",
      );
      await expectConstraintFailure(
        client,
        seeded.eventId,
        `status = 'blocked', attempt_count = 1, next_attempt_at = statement_timestamp(),
         last_error_code = 'BLOCKED', last_error_message = 'blocked',
         last_error_at = statement_timestamp()`,
      );
      await expectConstraintFailure(client, seeded.eventId, "attempt_count = -1");
      await expectConstraintFailure(client, seeded.eventId, "attempt_count = 20");
      await expectConstraintFailure(client, seeded.eventId, "attempt_count = 21");
    } finally {
      await client.query("ROLLBACK");
      client.release();
    }
  });

  it("rolls back claim and fenced transition updates when repository hooks fail", async () => {
    const [claimSeed] = await seedRevocationBoard();
    if (!claimSeed) throw new Error("Expected claim seed");
    const beforeClaim = await stateFor(claimSeed.eventId);
    const claimRollbackRepository = new OutboxRepository(pool, {
      afterClaimUpdate: () => Promise.reject(new Error("rollback claim")),
    });
    await expect(
      claimRollbackRepository.claimAvailable({ owner: "rollback-worker" }),
    ).rejects.toThrow("rollback claim");
    expect(await stateFor(claimSeed.eventId)).toEqual(beforeClaim);

    const [claim] = await outboxRepository.claimAvailable({ owner: "transition-worker" });
    if (!claim) throw new Error("Expected transition claim");
    const beforeFailure = await stateFor(claim.eventId);
    const transitionRollbackRepository = new OutboxRepository(pool, {
      afterLeaseMutationUpdate: () => Promise.reject(new Error("rollback transition")),
    });
    await expect(
      transitionRollbackRepository.recordFailure({
        eventId: claim.eventId,
        leaseToken: claim.leaseToken,
        retryable: false,
        errorCode: "FAIL",
        errorMessage: "must roll back",
      }),
    ).rejects.toThrow("rollback transition");
    expect(await stateFor(claim.eventId)).toEqual(beforeFailure);
  });
});
