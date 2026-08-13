import { BoardRepository, createPool, OutboxRepository } from "@converge/database";

async function main(): Promise<void> {
  const databaseUrl = process.env.DATABASE_URL;
  const userId = process.env.CONVERGE_K6_FIXTURE_USER_ID;
  if (!databaseUrl || !userId) throw new Error("Invalid fixture configuration");

  const pool = createPool(databaseUrl);
  try {
    if (process.env.CONVERGE_K6_FIXTURE_FORCE_FAILURE === "true")
      throw new Error("Forced fixture failure");
    const boards = new BoardRepository(pool);
    const outbox = new OutboxRepository(pool);
    const board = await boards.createBoard(userId, "M2.8 durable evidence preflight");
    const objectId = crypto.randomUUID();
    const committed = await boards.commitOperation(userId, {
      schemaVersion: 1,
      opId: crypto.randomUUID(),
      boardId: board.id,
      clientId: crypto.randomUUID(),
      baseSeq: 0,
      type: "object.create",
      targetId: objectId,
      payload: {
        id: objectId,
        kind: "rectangle",
        x: 0,
        y: 0,
        width: 8,
        height: 8,
        rotation: 0,
        fill: "#000000",
        text: "",
      },
      clientTimestamp: new Date().toISOString(),
    });

    const pending = await pool.query<{ status: string }>(
      "SELECT status FROM outbox_events WHERE id = $1",
      [committed.event.eventId],
    );
    if (pending.rows[0]?.status !== "pending")
      throw new Error("Repository fixture did not create pending evidence");

    const claims = await outbox.claimAvailable({
      owner: "m28-k6-preflight",
      batchSize: 1,
      leaseDurationMs: 60_000,
    });
    const claim = claims[0];
    if (!claim || claim.eventId !== committed.event.eventId)
      throw new Error("Repository fixture claim did not match committed evidence");

    const stale = await outbox.markPublished({
      eventId: claim.eventId,
      leaseToken: crypto.randomUUID(),
      publicationId: "1-0",
    });
    if (stale.outcome !== "stale")
      throw new Error("Repository fixture accepted a stale lease token");

    const published = await outbox.markPublished({
      eventId: claim.eventId,
      leaseToken: claim.leaseToken,
      publicationId: "1-0",
    });
    if (published.outcome !== "applied" || published.status !== "published")
      throw new Error("Repository fixture publication was not finalized");

    const terminal = await pool.query<{ valid: boolean }>(
      `SELECT status = 'published'
       AND next_attempt_at = 'infinity'::timestamptz
       AND redis_entry_id = '1-0'
       AND published_at IS NOT NULL
       AND lease_owner IS NULL
       AND lease_token IS NULL
       AND leased_until IS NULL
       AND last_error_code IS NULL
       AND last_error_message IS NULL
       AND last_error_at IS NULL AS valid
     FROM outbox_events WHERE id = $1`,
      [claim.eventId],
    );
    if (terminal.rows[0]?.valid !== true)
      throw new Error("Repository fixture terminal evidence is invalid");

    process.stdout.write(`${board.id}\n`);
  } finally {
    await pool.end();
  }
}

void main().catch(() => {
  process.stderr.write("Durable evidence fixture failed.\n");
  process.exitCode = 1;
});
