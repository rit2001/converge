import { readFile } from "node:fs/promises";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { BoardRepository, createPool, type BoardRepositoryHooks } from "@converge/database";
import { createRectangleCommand, fixtureIds } from "@converge/testkit";
import {
  operationCommittedDeliveryEnvelopeSchema,
  type DurableCommand,
  type OperationCommittedDeliveryEnvelope,
} from "@converge/protocol";

const databaseUrl =
  process.env.DATABASE_URL ?? "postgresql://converge:converge@localhost:55432/converge";
const pool = createPool(databaseUrl);
const repository = new BoardRepository(pool);

async function board(): Promise<string> {
  return (await repository.createBoard(fixtureIds.user, `receipt-${crypto.randomUUID()}`)).id;
}

async function durableState(boardId: string) {
  const result = await pool.query<{
    last_seq: string;
    last_delivery_seq: string;
    operation_count: string;
    projection_count: string;
    outbox_count: string;
    receipt_count: string;
  }>(
    `SELECT b.last_seq, b.last_delivery_seq,
            (SELECT count(*) FROM board_operations WHERE board_id = b.id) operation_count,
            (SELECT count(*) FROM board_objects WHERE board_id = b.id) projection_count,
            (SELECT count(*) FROM outbox_events WHERE board_id = b.id) outbox_count,
            (SELECT count(*) FROM board_operation_receipts WHERE board_id = b.id) receipt_count
     FROM boards b WHERE b.id = $1`,
    [boardId],
  );
  return result.rows[0];
}

function deferred(): { promise: Promise<void>; resolve: () => void } {
  let resolve = (): void => undefined;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

function operationEvent(
  command: DurableCommand,
  eventId = crypto.randomUUID(),
): OperationCommittedDeliveryEnvelope {
  const committedAt = new Date().toISOString();
  return operationCommittedDeliveryEnvelopeSchema.parse({
    schemaVersion: 1,
    eventId,
    boardId: command.boardId,
    deliverySeq: 1,
    eventType: "operation.committed",
    occurredAt: committedAt,
    payload: { operation: { ...command, seq: 1, committedAt } },
  });
}

async function expectInvalidReceipt(
  mutate: (evidence: { command: Record<string, unknown>; result: Record<string, unknown> }) => void,
): Promise<void> {
  const boardId = await board();
  const originalCommand = createRectangleCommand(boardId);
  const originalResult = operationEvent(originalCommand);
  const evidence = structuredClone({
    command: originalCommand as unknown as Record<string, unknown>,
    result: originalResult as unknown as Record<string, unknown>,
  });
  mutate(evidence);

  await expect(
    pool.query(
      `INSERT INTO board_operation_receipts(
         board_id, operation_id, actor_id, command, command_hash, hash_schema_version,
         canvas_seq, delivery_seq, event_id, committed_at, result
       ) VALUES (
         $1,$2,$3,$4,
         encode(
           sha256(convert_to('converge.operation-command.v1:' || $4::jsonb::text, 'UTF8')),
           'hex'
         ),
         1,1,1,$5,$6,$7
       )`,
      [
        boardId,
        originalCommand.opId,
        fixtureIds.user,
        evidence.command,
        originalResult.eventId,
        originalResult.occurredAt,
        evidence.result,
      ],
    ),
  ).rejects.toMatchObject({ code: "23514" });
}

beforeAll(async () => {
  await pool.query("SELECT 1");
});

afterAll(async () => {
  await pool.end();
});

describe("board-lifetime replay receipts", () => {
  it("atomically creates one receipt with the original acknowledgement metadata", async () => {
    const boardId = await board();
    const command = createRectangleCommand(boardId);
    const committed = await repository.commitOperation(fixtureIds.user, command);
    const receipt = await pool.query<{
      operation_id: string;
      actor_id: string;
      command: unknown;
      command_hash: string;
      hash_schema_version: number;
      canvas_seq: string;
      delivery_seq: string;
      event_id: string;
      committed_at: Date;
      result: unknown;
    }>("SELECT * FROM board_operation_receipts WHERE board_id = $1", [boardId]);

    expect(receipt.rowCount).toBe(1);
    expect(receipt.rows[0]).toMatchObject({
      operation_id: command.opId,
      actor_id: fixtureIds.user,
      command,
      hash_schema_version: 1,
      canvas_seq: String(committed.operation.seq),
      delivery_seq: String(committed.event.deliverySeq),
      event_id: committed.event.eventId,
      result: committed.event,
    });
    expect(receipt.rows[0]?.command_hash).toMatch(/^[0-9a-f]{64}$/);
    expect(receipt.rows[0]?.committed_at.toISOString()).toBe(committed.operation.committedAt);
  });

  it("uses the receipt for exact replay after controlled operation-row removal", async () => {
    const boardId = await board();
    const command = createRectangleCommand(boardId);
    const first = await repository.commitOperation(fixtureIds.user, command);
    await pool.query("DELETE FROM board_operations WHERE board_id = $1 AND op_id = $2", [
      boardId,
      command.opId,
    ]);
    const beforeReplay = await durableState(boardId);

    await expect(repository.commitOperation(fixtureIds.user, command)).resolves.toEqual({
      duplicate: true,
      operation: first.operation,
      event: first.event,
    });
    expect(await durableState(boardId)).toEqual(beforeReplay);
    expect(await durableState(boardId)).toMatchObject({
      operation_count: "0",
      outbox_count: "1",
      receipt_count: "1",
    });
  });

  it("rejects conflicting operation-id reuse permanently after operation-row removal", async () => {
    const boardId = await board();
    const command = createRectangleCommand(boardId);
    await repository.commitOperation(fixtureIds.user, command);
    await pool.query("DELETE FROM board_operations WHERE board_id = $1 AND op_id = $2", [
      boardId,
      command.opId,
    ]);
    const beforeConflict = await durableState(boardId);

    await expect(
      repository.commitOperation(fixtureIds.user, {
        ...command,
        clientTimestamp: new Date(Date.now() + 1_000).toISOString(),
      }),
    ).rejects.toMatchObject({ code: "IDEMPOTENCY_CONFLICT" });
    expect(await durableState(boardId)).toEqual(beforeConflict);
  });

  it("serializes concurrent identical submissions into one commit and one receipt", async () => {
    const boardId = await board();
    const command = createRectangleCommand(boardId);
    const results = await Promise.all([
      repository.commitOperation(fixtureIds.user, command),
      repository.commitOperation(fixtureIds.user, command),
    ]);

    expect(results.map(({ duplicate }) => duplicate).sort()).toEqual([false, true]);
    expect(results[0]?.operation).toEqual(results[1]?.operation);
    expect(results[0]?.event).toEqual(results[1]?.event);
    expect(await durableState(boardId)).toMatchObject({
      last_seq: "1",
      last_delivery_seq: "1",
      operation_count: "1",
      projection_count: "1",
      outbox_count: "1",
      receipt_count: "1",
    });
  });

  it("gives concurrent conflicting submissions one winner and one deterministic conflict", async () => {
    const boardId = await board();
    const operationId = crypto.randomUUID();
    const firstCommand = createRectangleCommand(boardId, crypto.randomUUID(), operationId);
    const secondCommand = createRectangleCommand(boardId, crypto.randomUUID(), operationId);
    const firstLocked = deferred();
    const releaseFirst = deferred();
    const secondAttempting = deferred();
    const hooks: BoardRepositoryHooks = {
      beforeSequenceLock: (command) => {
        if (command.targetId === secondCommand.targetId) secondAttempting.resolve();
        return Promise.resolve();
      },
      afterSequenceLock: async (command) => {
        if (command.targetId !== firstCommand.targetId) return;
        firstLocked.resolve();
        await releaseFirst.promise;
      },
    };
    const lockedRepository = new BoardRepository(pool, hooks);

    const first = lockedRepository.commitOperation(fixtureIds.user, firstCommand);
    await firstLocked.promise;
    const second = lockedRepository.commitOperation(fixtureIds.user, secondCommand);
    await secondAttempting.promise;
    releaseFirst.resolve();

    await expect(first).resolves.toMatchObject({ duplicate: false, operation: firstCommand });
    await expect(second).rejects.toMatchObject({ code: "IDEMPOTENCY_CONFLICT" });
    expect(await durableState(boardId)).toMatchObject({
      last_seq: "1",
      operation_count: "1",
      projection_count: "1",
      outbox_count: "1",
      receipt_count: "1",
    });
  });

  it("rolls back the receipt and every operation mutation on a forced pre-commit failure", async () => {
    const boardId = await board();
    const failingRepository = new BoardRepository(pool, {
      afterReceiptInsert: () => Promise.reject(new Error("forced receipt transaction failure")),
    });
    await expect(
      failingRepository.commitOperation(fixtureIds.user, createRectangleCommand(boardId)),
    ).rejects.toThrow("forced receipt transaction failure");
    expect(await durableState(boardId)).toMatchObject({
      last_seq: "0",
      last_delivery_seq: "0",
      operation_count: "0",
      projection_count: "0",
      outbox_count: "0",
      receipt_count: "0",
    });
  });

  it("rejects missing, null, malformed, mismatched, and unknown JSON metadata", async () => {
    await expectInvalidReceipt(({ result }) => {
      delete result.eventId;
    });
    await expectInvalidReceipt(({ result }) => {
      result.eventId = null;
    });
    await expectInvalidReceipt(({ command }) => {
      command.baseSeq = "malformed";
    });
    await expectInvalidReceipt(({ result }) => {
      result.deliverySeq = 2;
    });
    await expectInvalidReceipt(({ command }) => {
      command.unknownMetadata = true;
    });
  });

  it("prevents every receipt content update", async () => {
    const boardId = await board();
    await repository.commitOperation(fixtureIds.user, createRectangleCommand(boardId));
    await expect(
      pool.query("UPDATE board_operation_receipts SET actor_id = $2 WHERE board_id = $1", [
        boardId,
        crypto.randomUUID(),
      ]),
    ).rejects.toMatchObject({ code: "55000" });
  });
});

describe("recovery floors", () => {
  it("initializes both floors at zero without changing board heads", async () => {
    const boardId = await board();
    const heads = await pool.query(
      `SELECT last_seq, last_delivery_seq, operation_recovery_floor, delivery_recovery_floor
       FROM boards WHERE id = $1`,
      [boardId],
    );
    expect(heads.rows[0]).toEqual({
      last_seq: "0",
      last_delivery_seq: "0",
      operation_recovery_floor: "0",
      delivery_recovery_floor: "0",
    });
  });

  it("allows only forward floor movement within the authoritative heads", async () => {
    const boardId = await board();
    await repository.commitOperation(fixtureIds.user, createRectangleCommand(boardId));
    await pool.query(
      `UPDATE boards
       SET operation_recovery_floor = 1, delivery_recovery_floor = 1
       WHERE id = $1`,
      [boardId],
    );
    await expect(
      pool.query("UPDATE boards SET operation_recovery_floor = 0 WHERE id = $1", [boardId]),
    ).rejects.toMatchObject({ code: "55000" });
    await expect(
      pool.query("UPDATE boards SET operation_recovery_floor = 2 WHERE id = $1", [boardId]),
    ).rejects.toMatchObject({ code: "23514" });
    await expect(
      pool.query("UPDATE boards SET delivery_recovery_floor = 2 WHERE id = $1", [boardId]),
    ).rejects.toMatchObject({ code: "23514" });
    await expect(
      pool.query("UPDATE boards SET delivery_recovery_floor = -1 WHERE id = $1", [boardId]),
    ).rejects.toMatchObject({ code: "55000" });
  });
});

describe("receipt migration backfill", () => {
  it("backfills every existing operation one-to-one without fabricated metadata", async () => {
    const migration = await readFile(
      new URL(
        "../../packages/database/migrations/0009_board_replay_receipts_and_recovery_floors.sql",
        import.meta.url,
      ),
      "utf8",
    );
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      const schema = `m27_backfill_${crypto.randomUUID().replaceAll("-", "")}`;
      await client.query(`CREATE SCHEMA "${schema}"`);
      await client.query(`SET LOCAL search_path TO "${schema}"`);
      await client.query(`
        CREATE TABLE boards (
          id uuid PRIMARY KEY,
          last_seq bigint NOT NULL DEFAULT 0,
          last_delivery_seq bigint NOT NULL DEFAULT 0
        );
        CREATE TABLE board_operations (
          board_id uuid NOT NULL,
          seq bigint NOT NULL,
          op_id uuid NOT NULL,
          user_id uuid NOT NULL,
          event_id uuid NOT NULL,
          delivery_seq bigint NOT NULL,
          command jsonb NOT NULL,
          committed_at timestamptz NOT NULL,
          PRIMARY KEY (board_id, seq),
          UNIQUE (board_id, op_id)
        );
        CREATE TABLE outbox_events (
          id uuid PRIMARY KEY,
          board_id uuid NOT NULL,
          delivery_seq bigint NOT NULL,
          canvas_seq bigint,
          event_type text NOT NULL,
          payload jsonb NOT NULL
        );
      `);
      const boardId = crypto.randomUUID();
      const command = createRectangleCommand(boardId);
      const event = operationEvent(command);
      await client.query("INSERT INTO boards(id, last_seq, last_delivery_seq) VALUES ($1, 1, 1)", [
        boardId,
      ]);
      await client.query(
        `INSERT INTO board_operations(
           board_id, seq, op_id, user_id, event_id, delivery_seq, command, committed_at
         ) VALUES ($1,1,$2,$3,$4,1,$5,$6)`,
        [boardId, command.opId, fixtureIds.user, event.eventId, command, event.occurredAt],
      );
      await client.query(
        `INSERT INTO outbox_events(
           id, board_id, delivery_seq, canvas_seq, event_type, payload
         ) VALUES ($1,$2,1,1,'operation.committed',$3)`,
        [event.eventId, boardId, event],
      );

      await client.query(migration);
      const parity = await client.query(
        `SELECT operation.op_id, receipt.operation_id, receipt.event_id,
                receipt.canvas_seq, receipt.delivery_seq, receipt.committed_at,
                board.operation_recovery_floor, board.delivery_recovery_floor
         FROM board_operations operation
         JOIN board_operation_receipts receipt
           ON receipt.board_id = operation.board_id AND receipt.operation_id = operation.op_id
         JOIN boards board ON board.id = operation.board_id`,
      );
      expect(parity.rows).toEqual([
        expect.objectContaining({
          op_id: command.opId,
          operation_id: command.opId,
          event_id: event.eventId,
          canvas_seq: "1",
          delivery_seq: "1",
          operation_recovery_floor: "0",
          delivery_recovery_floor: "0",
        }),
      ]);
    } finally {
      await client.query("ROLLBACK");
      client.release();
    }
  });
});
