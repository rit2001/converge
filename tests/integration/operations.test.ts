import { readFile } from "node:fs/promises";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
  applyCommitted,
  emptyBoardState,
  hashBoardState,
  visibleObjects,
} from "@converge/canvas-engine";
import { BoardRepository, createPool } from "@converge/database";
import { createRectangleCommand, fixtureIds } from "@converge/testkit";
import {
  boardSnapshotSchema,
  deliveryEnvelopeSchema,
  operationCommittedDeliveryEnvelopeSchema,
  type BoardSnapshot,
  type DurableCommand,
} from "@converge/protocol";

const databaseUrl =
  process.env.DATABASE_URL ?? "postgresql://converge:converge@localhost:55432/converge";
const pool = createPool(databaseUrl);
const repository = new BoardRepository(pool);

async function board(): Promise<string> {
  return (await repository.createBoard(fixtureIds.user, `integration-${crypto.randomUUID()}`)).id;
}

async function durableState(boardId: string) {
  const result = await pool.query<{
    last_seq: string;
    last_delivery_seq: string;
    operation_count: string;
    projection: unknown;
    membership_count: string;
    outbox_count: string;
  }>(
    `SELECT b.last_seq, b.last_delivery_seq,
            (SELECT count(*) FROM board_operations WHERE board_id = b.id) operation_count,
            COALESCE(
              (SELECT jsonb_agg(
                 jsonb_build_object(
                   'objectId', o.object_id,
                   'objectData', o.object_data,
                   'fieldSeq', o.field_seq,
                   'createdSeq', o.created_seq,
                   'stackOrder', o.stack_order,
                   'updatedSeq', o.updated_seq,
                   'deletedSeq', o.deleted_seq
                 ) ORDER BY o.object_id
               ) FROM board_objects o WHERE o.board_id = b.id),
              '[]'::jsonb
            ) projection,
            (SELECT count(*) FROM board_members WHERE board_id = b.id) membership_count,
            (SELECT count(*) FROM outbox_events WHERE board_id = b.id) outbox_count
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

function stateFromSnapshot(snapshot: BoardSnapshot) {
  const state = emptyBoardState();
  state.lastSeq = snapshot.lastSeq;
  for (const value of snapshot.objects) {
    state.objects[value.id] = {
      value,
      createdSeq: 0,
      updatedSeq: snapshot.lastSeq,
      deletedSeq: null,
      fieldSeq: {},
    };
    state.order.push(value.id);
  }
  return state;
}

beforeAll(async () => {
  await pool.query("SELECT 1");
});
afterAll(async () => {
  await pool.end();
});

describe("authoritative operation transactions", () => {
  it("starts a new board with zero canvas and delivery heads", async () => {
    const boardId = await board();
    const heads = await pool.query<{ last_seq: string; last_delivery_seq: string }>(
      "SELECT last_seq, last_delivery_seq FROM boards WHERE id = $1",
      [boardId],
    );
    expect(heads.rows[0]).toEqual({ last_seq: "0", last_delivery_seq: "0" });
  });

  it("backfills stack order from created sequence for existing projections", async () => {
    const migration = await readFile(
      new URL(
        "../../packages/database/migrations/0003_authoritative_stacking_order.sql",
        import.meta.url,
      ),
      "utf8",
    );
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      await client.query(
        `CREATE TEMP TABLE board_objects (
           board_id uuid NOT NULL,
           object_id uuid NOT NULL,
           created_seq bigint NOT NULL,
           PRIMARY KEY (board_id, object_id)
         )`,
      );
      await client.query("SET LOCAL search_path TO pg_temp");
      const boardId = crypto.randomUUID();
      await client.query(
        `INSERT INTO board_objects(board_id, object_id, created_seq)
         VALUES ($1, $2, 7), ($1, $3, 3)`,
        [boardId, "f0000000-0000-4000-8000-000000000001", "10000000-0000-4000-8000-000000000002"],
      );

      await client.query(migration);
      const rows = await client.query<{ created_seq: string; stack_order: string }>(
        "SELECT created_seq, stack_order FROM board_objects ORDER BY stack_order",
      );
      expect(rows.rows).toEqual([
        { created_seq: "3", stack_order: "3" },
        { created_seq: "7", stack_order: "7" },
      ]);
    } finally {
      await client.query("ROLLBACK");
      client.release();
    }
  });

  it("backfills drained M1 outbox rows with deterministic historical delivery ordering", async () => {
    const orderingMigration = await readFile(
      new URL(
        "../../packages/database/migrations/0004_durable_board_delivery_ordering.sql",
        import.meta.url,
      ),
      "utf8",
    );
    const constraintMigration = await readFile(
      new URL(
        "../../packages/database/migrations/0005_complete_outbox_envelope_constraints.sql",
        import.meta.url,
      ),
      "utf8",
    );
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      await client.query("SET LOCAL search_path TO pg_temp");
      await client.query(`
        CREATE TEMP TABLE boards (
          id uuid PRIMARY KEY,
          name text NOT NULL,
          created_by uuid NOT NULL,
          last_seq bigint NOT NULL DEFAULT 0,
          created_at timestamptz NOT NULL DEFAULT now(),
          updated_at timestamptz NOT NULL DEFAULT now()
        );
        CREATE TEMP TABLE board_operations (
          board_id uuid NOT NULL,
          seq bigint NOT NULL,
          op_id uuid NOT NULL,
          client_id uuid NOT NULL,
          user_id uuid NOT NULL,
          base_seq bigint NOT NULL,
          type text NOT NULL,
          target_id uuid NOT NULL,
          command jsonb NOT NULL,
          committed_at timestamptz NOT NULL,
          PRIMARY KEY (board_id, seq),
          UNIQUE (board_id, op_id)
        );
        CREATE TEMP TABLE outbox_events (
          id uuid PRIMARY KEY,
          board_id uuid NOT NULL,
          board_seq bigint,
          event_type text NOT NULL,
          payload jsonb NOT NULL,
          attempts integer NOT NULL DEFAULT 0,
          created_at timestamptz NOT NULL,
          published_at timestamptz
        );
        CREATE UNIQUE INDEX outbox_operation_board_seq_uq
          ON outbox_events(board_id, board_seq)
          WHERE event_type = 'operation.committed';
      `);

      const boardA = "a0000000-0000-4000-8000-000000000001";
      const boardB = "b0000000-0000-4000-8000-000000000001";
      const operationA1 = {
        ...createRectangleCommand(boardA),
        seq: 1,
        committedAt: "2026-08-07T12:00:02.000Z",
      };
      const operationA2 = {
        ...createRectangleCommand(boardA),
        baseSeq: 1,
        seq: 2,
        committedAt: "2026-08-07T12:00:03.000Z",
      };
      const operationB1 = {
        ...createRectangleCommand(boardB),
        seq: 1,
        committedAt: "2026-08-07T12:00:04.000Z",
      };
      await client.query(
        `INSERT INTO boards(id, name, created_by, last_seq)
         VALUES ($1, 'Legacy A', $3, 2), ($2, 'Legacy B', $3, 1)`,
        [boardA, boardB, fixtureIds.user],
      );
      for (const operation of [operationA1, operationA2, operationB1]) {
        await client.query(
          `INSERT INTO board_operations(
             board_id, seq, op_id, client_id, user_id, base_seq, type, target_id, command,
             committed_at
           ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
          [
            operation.boardId,
            operation.seq,
            operation.opId,
            operation.clientId,
            fixtureIds.user,
            operation.baseSeq,
            operation.type,
            operation.targetId,
            {
              schemaVersion: operation.schemaVersion,
              opId: operation.opId,
              boardId: operation.boardId,
              clientId: operation.clientId,
              baseSeq: operation.baseSeq,
              type: operation.type,
              targetId: operation.targetId,
              payload: operation.payload,
              clientTimestamp: operation.clientTimestamp,
            },
            operation.committedAt,
          ],
        );
      }
      const operationEventIds = [
        "a1000000-0000-4000-8000-000000000001",
        "a1000000-0000-4000-8000-000000000002",
        "b1000000-0000-4000-8000-000000000001",
      ];
      await client.query(
        `INSERT INTO outbox_events(
           id, board_id, board_seq, event_type, payload, created_at
         ) VALUES
           ($1, $4, 1, 'operation.committed', $7, '2026-08-07T12:00:02Z'),
           ($2, $4, 2, 'operation.committed', $8, '2026-08-07T12:00:03Z'),
           ($3, $5, 1, 'operation.committed', $9, '2026-08-07T12:00:04Z'),
           ($6, $4, NULL, 'board.membership.revoked', $10, '2026-08-07T12:00:01Z')`,
        [
          ...operationEventIds,
          boardA,
          boardB,
          "a2000000-0000-4000-8000-000000000001",
          operationA1,
          operationA2,
          operationB1,
          {
            schemaVersion: 1,
            eventId: "a2000000-0000-4000-8000-000000000001",
            kind: "board.membership.revoked",
            boardId: boardA,
            revokedUserId: "a3000000-0000-4000-8000-000000000001",
            initiatedByUserId: fixtureIds.user,
            committedAt: "2026-08-07T12:00:01.000Z",
          },
        ],
      );

      await client.query(orderingMigration);
      await client.query(constraintMigration);

      const events = await client.query<{
        id: string;
        board_id: string;
        delivery_seq: string;
        canvas_seq: string | null;
        published_at: Date | null;
        payload: unknown;
      }>(
        `SELECT id, board_id, delivery_seq, canvas_seq, published_at, payload
         FROM outbox_events ORDER BY board_id, delivery_seq`,
      );
      expect(
        events.rows.map(({ board_id, delivery_seq, canvas_seq, published_at }) => ({
          board_id,
          delivery_seq,
          canvas_seq,
          published: published_at !== null,
        })),
      ).toEqual([
        { board_id: boardA, delivery_seq: "1", canvas_seq: "1", published: true },
        { board_id: boardA, delivery_seq: "2", canvas_seq: "2", published: true },
        { board_id: boardA, delivery_seq: "3", canvas_seq: null, published: true },
        { board_id: boardB, delivery_seq: "1", canvas_seq: "1", published: true },
      ]);
      for (const event of events.rows)
        expect(deliveryEnvelopeSchema.parse(event.payload)).toBeDefined();

      const heads = await client.query<{ id: string; last_delivery_seq: string }>(
        "SELECT id, last_delivery_seq FROM boards ORDER BY id",
      );
      expect(heads.rows).toEqual([
        { id: boardA, last_delivery_seq: "3" },
        { id: boardB, last_delivery_seq: "1" },
      ]);
      const mappings = await client.query<{
        event_id: string;
        delivery_seq: string;
      }>("SELECT event_id, delivery_seq FROM board_operations ORDER BY board_id, seq");
      expect(mappings.rows).toEqual([
        { event_id: operationEventIds[0], delivery_seq: "1" },
        { event_id: operationEventIds[1], delivery_seq: "2" },
        { event_id: operationEventIds[2], delivery_seq: "1" },
      ]);
    } finally {
      await client.query("ROLLBACK");
      client.release();
    }
  });

  it("persists creation stacking across live replay, snapshots, mutations, and exact replay", async () => {
    const boardId = await board();
    const highId = "f0000000-0000-4000-8000-000000000001";
    const lowId = "10000000-0000-4000-8000-000000000002";
    const high = createRectangleCommand(boardId, highId);
    const low = { ...createRectangleCommand(boardId, lowId), baseSeq: 1 };
    const first = await repository.commitOperation(fixtureIds.user, high);
    const second = await repository.commitOperation(fixtureIds.user, low);
    const live = applyCommitted(
      applyCommitted(emptyBoardState(), first.operation),
      second.operation,
    );
    const afterCreate = await repository.getBoard(boardId, fixtureIds.user);

    expect(visibleObjects(live).map(({ id }) => id)).toEqual([highId, lowId]);
    expect(afterCreate.objects.map(({ id }) => id)).toEqual([highId, lowId]);
    expect(await hashBoardState(live)).toBe(await hashBoardState(stateFromSnapshot(afterCreate)));
    const ranks = await pool.query<{
      object_id: string;
      created_seq: string;
      stack_order: string;
    }>(
      `SELECT object_id, created_seq, stack_order
       FROM board_objects WHERE board_id = $1 ORDER BY stack_order, object_id`,
      [boardId],
    );
    expect(ranks.rows).toEqual([
      { object_id: highId, created_seq: "1", stack_order: "1" },
      { object_id: lowId, created_seq: "2", stack_order: "2" },
    ]);

    const beforeReplay = await durableState(boardId);
    await expect(repository.commitOperation(fixtureIds.user, low)).resolves.toMatchObject({
      duplicate: true,
      operation: { opId: low.opId, seq: 2 },
    });
    expect(await durableState(boardId)).toEqual(beforeReplay);

    const update: DurableCommand = {
      ...high,
      opId: crypto.randomUUID(),
      baseSeq: 2,
      type: "object.update",
      payload: { fill: "#ffffff" },
    };
    await repository.commitOperation(fixtureIds.user, update);
    const transform: DurableCommand = {
      ...low,
      opId: crypto.randomUUID(),
      baseSeq: 3,
      type: "object.transform",
      payload: { x: 80, y: 90 },
    };
    await repository.commitOperation(fixtureIds.user, transform);
    expect(
      (await repository.getBoard(boardId, fixtureIds.user)).objects.map(({ id }) => id),
    ).toEqual([highId, lowId]);
    expect(
      (
        await pool.query<{ object_id: string; stack_order: string }>(
          `SELECT object_id, stack_order FROM board_objects
           WHERE board_id = $1 ORDER BY stack_order, object_id`,
          [boardId],
        )
      ).rows,
    ).toEqual([
      { object_id: highId, stack_order: "1" },
      { object_id: lowId, stack_order: "2" },
    ]);

    await expect(
      pool.query(
        "UPDATE board_objects SET stack_order = 1 WHERE board_id = $1 AND object_id = $2",
        [boardId, lowId],
      ),
    ).rejects.toMatchObject({ code: "23505" });

    const removeBottom: DurableCommand = {
      ...high,
      opId: crypto.randomUUID(),
      baseSeq: 4,
      type: "object.delete",
      payload: {},
    };
    await repository.commitOperation(fixtureIds.user, removeBottom);
    expect(
      (await repository.getBoard(boardId, fixtureIds.user)).objects.map(({ id }) => id),
    ).toEqual([lowId]);
    expect(
      (
        await pool.query<{ object_id: string; stack_order: string }>(
          `SELECT object_id, stack_order FROM board_objects
           WHERE board_id = $1 ORDER BY stack_order, object_id`,
          [boardId],
        )
      ).rows,
    ).toEqual([
      { object_id: highId, stack_order: "1" },
      { object_id: lowId, stack_order: "2" },
    ]);
  });

  it("advances both heads and atomically persists one typed operation event", async () => {
    const boardId = await board();
    const first = await repository.commitOperation(
      fixtureIds.user,
      createRectangleCommand(boardId),
    );
    const second = await repository.commitOperation(
      fixtureIds.user,
      createRectangleCommand(boardId),
    );
    expect([first.operation.seq, second.operation.seq]).toEqual([1, 2]);
    expect([first.event.deliverySeq, second.event.deliverySeq]).toEqual([1, 2]);
    expect(first.event).toMatchObject({
      eventType: "operation.committed",
      boardId,
      deliverySeq: 1,
      payload: { operation: first.operation },
    });
    const persisted = await pool.query<{
      last_seq: string;
      last_delivery_seq: string;
      operation_event_id: string;
      operation_delivery_seq: string;
      outbox_event_id: string;
      outbox_delivery_seq: string;
      canvas_seq: string;
      schema_version: number;
      payload: unknown;
    }>(
      `SELECT b.last_seq, b.last_delivery_seq,
              operation.event_id operation_event_id,
              operation.delivery_seq operation_delivery_seq,
              event.id outbox_event_id,
              event.delivery_seq outbox_delivery_seq,
              event.canvas_seq,
              event.schema_version,
              event.payload
       FROM boards b
       JOIN board_operations operation ON operation.board_id = b.id AND operation.seq = 1
       JOIN outbox_events event ON event.id = operation.event_id
       WHERE b.id = $1`,
      [boardId],
    );
    expect(persisted.rows[0]).toMatchObject({
      last_seq: "2",
      last_delivery_seq: "2",
      operation_event_id: first.event.eventId,
      operation_delivery_seq: "1",
      outbox_event_id: first.event.eventId,
      outbox_delivery_seq: "1",
      canvas_seq: "1",
      schema_version: 1,
    });
    expect(operationCommittedDeliveryEnvelopeSchema.parse(persisted.rows[0]?.payload)).toEqual(
      first.event,
    );
  });

  it("keeps delivery sequences independent across boards", async () => {
    const boardA = await board();
    const boardB = await board();
    const [eventA, eventB] = await Promise.all([
      repository.commitOperation(fixtureIds.user, createRectangleCommand(boardA)),
      repository.commitOperation(fixtureIds.user, createRectangleCommand(boardB)),
    ]);
    expect(eventA.event.deliverySeq).toBe(1);
    expect(eventB.event.deliverySeq).toBe(1);
    const heads = await pool.query<{ id: string; last_delivery_seq: string }>(
      "SELECT id, last_delivery_seq FROM boards WHERE id = ANY($1::uuid[]) ORDER BY id",
      [[boardA, boardB]],
    );
    expect(heads.rows.map((row) => row.last_delivery_seq)).toEqual(["1", "1"]);
  });

  it("rejects a duplicate authoritative board delivery sequence", async () => {
    const boardId = await board();
    const committed = await repository.commitOperation(
      fixtureIds.user,
      createRectangleCommand(boardId),
    );
    const duplicateId = crypto.randomUUID();
    const duplicate = {
      ...committed.event,
      eventId: duplicateId,
      eventType: "board.membership.revoked" as const,
      payload: { revokedUserId: fixtureIds.user, initiatedByUserId: fixtureIds.user },
    };
    const before = await durableState(boardId);
    await expect(
      pool.query(
        `INSERT INTO outbox_events(
           id, board_id, delivery_seq, canvas_seq, event_type, schema_version, payload
         ) VALUES ($1, $2, $3, NULL, 'board.membership.revoked', 1, $4)`,
        [duplicateId, boardId, committed.event.deliverySeq, duplicate],
      ),
    ).rejects.toMatchObject({ code: "23505" });
    expect(await durableState(boardId)).toEqual(before);
  });

  it("returns the original acknowledgement for duplicate opId without duplicating state", async () => {
    const boardId = await board();
    const command = createRectangleCommand(boardId);
    const first = await repository.commitOperation(fixtureIds.user, command);
    const afterFirst = await durableState(boardId);
    const duplicate = await repository.commitOperation(fixtureIds.user, command);
    expect(duplicate).toMatchObject({
      duplicate: true,
      operation: { seq: first.operation.seq, opId: command.opId },
      event: { eventId: first.event.eventId, deliverySeq: first.event.deliverySeq },
    });
    expect(await durableState(boardId)).toEqual(afterFirst);
  });

  it("rejects a future base sequence without changing durable state", async () => {
    const boardId = await board();
    const before = await durableState(boardId);
    const command = { ...createRectangleCommand(boardId), baseSeq: 9_000 };
    await expect(repository.commitOperation(fixtureIds.user, command)).rejects.toMatchObject({
      code: "RESYNC_REQUIRED",
    });
    expect(await durableState(boardId)).toEqual(before);
  });

  it("rejects caller-supplied delivery metadata before changing durable state", async () => {
    const boardId = await board();
    const before = await durableState(boardId);
    const command = {
      ...createRectangleCommand(boardId),
      deliverySeq: 99,
      eventId: crypto.randomUUID(),
      committedAt: new Date().toISOString(),
    } as unknown as DurableCommand;
    await expect(repository.commitOperation(fixtureIds.user, command)).rejects.toMatchObject({
      name: "ZodError",
    });
    expect(await durableState(boardId)).toEqual(before);
  });

  it("checks a waiting command against the board head after acquiring the sequence lock", async () => {
    const boardId = await board();
    const firstCommand = createRectangleCommand(boardId);
    const secondCommand = { ...createRectangleCommand(boardId), baseSeq: 1 };
    const firstLocked = deferred();
    const releaseFirst = deferred();
    const secondAttemptingLock = deferred();
    const lockedRepository = new BoardRepository(pool, {
      beforeSequenceLock: (command) => {
        if (command.opId === secondCommand.opId) secondAttemptingLock.resolve();
        return Promise.resolve();
      },
      afterSequenceLock: async (command) => {
        if (command.opId !== firstCommand.opId) return;
        firstLocked.resolve();
        await releaseFirst.promise;
      },
    });

    const first = lockedRepository.commitOperation(fixtureIds.user, firstCommand);
    await firstLocked.promise;
    const second = lockedRepository.commitOperation(fixtureIds.user, secondCommand);
    await secondAttemptingLock.promise;
    releaseFirst.resolve();

    await expect(Promise.all([first, second])).resolves.toMatchObject([
      { operation: { seq: 1 } },
      { operation: { seq: 2 } },
    ]);
    expect(await durableState(boardId)).toMatchObject({
      last_seq: "2",
      operation_count: "2",
      outbox_count: "2",
    });
  });

  it("treats JSON key-order differences as an exact idempotent replay", async () => {
    const boardId = await board();
    const command = createRectangleCommand(boardId);
    if (command.type !== "object.create" || command.payload.kind !== "rectangle")
      throw new Error("Expected rectangle create command");
    const first = await repository.commitOperation(fixtureIds.user, command);
    const afterFirst = await durableState(boardId);
    const value = command.payload;
    const reordered: DurableCommand = {
      clientTimestamp: command.clientTimestamp,
      payload: {
        text: value.text,
        fill: value.fill,
        rotation: value.rotation,
        height: value.height,
        width: value.width,
        y: value.y,
        x: value.x,
        kind: value.kind,
        id: value.id,
      },
      targetId: command.targetId,
      type: command.type,
      baseSeq: command.baseSeq,
      clientId: command.clientId,
      boardId: command.boardId,
      opId: command.opId,
      schemaVersion: command.schemaVersion,
    };

    await expect(repository.commitOperation(fixtureIds.user, reordered)).resolves.toMatchObject({
      duplicate: true,
      operation: { opId: command.opId, seq: first.operation.seq },
    });
    expect(await durableState(boardId)).toEqual(afterFirst);
  });

  it("rejects operation-id reuse across every semantic intent dimension", async () => {
    const boardId = await board();
    const editorA = "00000000-0000-4000-8000-000000000011";
    const editorB = "00000000-0000-4000-8000-000000000012";
    await pool.query(
      `INSERT INTO board_members(board_id, user_id, role)
       VALUES ($1, $2, 'editor'), ($1, $3, 'editor')`,
      [boardId, editorA, editorB],
    );
    const firstObject = createRectangleCommand(boardId);
    const secondObject = createRectangleCommand(boardId);
    await repository.commitOperation(fixtureIds.user, firstObject);
    await repository.commitOperation(fixtureIds.user, secondObject);
    const original: DurableCommand = {
      ...firstObject,
      opId: crypto.randomUUID(),
      baseSeq: 2,
      type: "object.update",
      payload: { fill: "#ffffff" },
      clientTimestamp: "2026-08-07T12:00:00.000Z",
    };
    await repository.commitOperation(editorA, original);
    const afterOriginal = await durableState(boardId);
    const attempts: Array<{ actorId: string; command: DurableCommand }> = [
      {
        actorId: editorA,
        command: { ...original, type: "object.transform", payload: { x: 80 } },
      },
      { actorId: editorA, command: { ...original, targetId: secondObject.targetId } },
      { actorId: editorA, command: { ...original, payload: { fill: "#000000" } } },
      { actorId: editorA, command: { ...original, baseSeq: 1 } },
      { actorId: editorB, command: original },
    ];

    for (const attempt of attempts) {
      await expect(
        repository.commitOperation(attempt.actorId, attempt.command),
      ).rejects.toMatchObject({ code: "IDEMPOTENCY_CONFLICT" });
      expect(await durableState(boardId)).toEqual(afterOriginal);
    }
  });

  it("reauthorizes a removed editor before returning an exact replay", async () => {
    const boardId = await board();
    const editor = "00000000-0000-4000-8000-000000000013";
    await pool.query(
      "INSERT INTO board_members(board_id, user_id, role) VALUES ($1, $2, 'editor')",
      [boardId, editor],
    );
    const command = createRectangleCommand(boardId);
    await repository.commitOperation(editor, command);
    await pool.query("DELETE FROM board_members WHERE board_id = $1 AND user_id = $2", [
      boardId,
      editor,
    ]);
    const beforeRemovalRetry = await durableState(boardId);

    await expect(repository.commitOperation(editor, command)).rejects.toMatchObject({
      code: "FORBIDDEN",
    });
    expect(await durableState(boardId)).toEqual(beforeRemovalRetry);
  });

  it("preserves concurrent operations on separate objects", async () => {
    const boardId = await board();
    const [left, right] = await Promise.all([
      repository.commitOperation(fixtureIds.user, createRectangleCommand(boardId)),
      repository.commitOperation(fixtureIds.user, createRectangleCommand(boardId)),
    ]);
    expect(new Set([left.operation.seq, right.operation.seq])).toEqual(new Set([1, 2]));
    expect((await repository.getBoard(boardId, fixtureIds.user)).objects).toHaveLength(2);
  });

  it("rejects a stale update after delete", async () => {
    const boardId = await board();
    const create = createRectangleCommand(boardId);
    await repository.commitOperation(fixtureIds.user, create);
    const base = { ...create, baseSeq: 1, clientTimestamp: new Date().toISOString() };
    await repository.commitOperation(fixtureIds.user, {
      ...base,
      opId: crypto.randomUUID(),
      type: "object.delete",
      payload: {},
    });
    const stale: DurableCommand = {
      ...base,
      baseSeq: 1,
      opId: crypto.randomUUID(),
      type: "object.update",
      payload: { fill: "#ffffff" },
    };
    await expect(repository.commitOperation(fixtureIds.user, stale)).rejects.toMatchObject({
      code: "TARGET_DELETED",
    });
  });

  it("fetches a bounded missing sequence range in order", async () => {
    const boardId = await board();
    await repository.commitOperation(fixtureIds.user, createRectangleCommand(boardId));
    await repository.commitOperation(fixtureIds.user, createRectangleCommand(boardId));
    await repository.commitOperation(fixtureIds.user, createRectangleCommand(boardId));
    const operations = await repository.getOperations(boardId, fixtureIds.user, 2, 3);
    expect(operations.map((operation) => operation.seq)).toEqual([2, 3]);
  });

  it("rolls back a kind-incompatible patch and accepts the next valid mutation", async () => {
    const boardId = await board();
    const create = createRectangleCommand(boardId);
    await repository.commitOperation(fixtureIds.user, create);

    const persistedState = async () => {
      const result = await pool.query<{
        last_seq: string;
        operation_count: string;
        object_data: unknown;
        outbox_count: string;
      }>(
        `SELECT b.last_seq,
                (SELECT count(*) FROM board_operations WHERE board_id = b.id) operation_count,
                o.object_data,
                (SELECT count(*) FROM outbox_events WHERE board_id = b.id) outbox_count
         FROM boards b
         JOIN board_objects o ON o.board_id = b.id AND o.object_id = $2
         WHERE b.id = $1`,
        [boardId, create.targetId],
      );
      return result.rows[0];
    };

    const before = await persistedState();
    expect(before).toBeDefined();
    const invalid: DurableCommand = {
      ...create,
      opId: crypto.randomUUID(),
      baseSeq: 1,
      type: "object.update",
      payload: { text: "rectangle poisoning" },
      clientTimestamp: new Date().toISOString(),
    };
    await expect(repository.commitOperation(fixtureIds.user, invalid)).rejects.toMatchObject({
      code: "INVALID_COMMAND",
    });

    expect(await persistedState()).toEqual(before);
    expect(boardSnapshotSchema.parse(await repository.getBoard(boardId, fixtureIds.user))).toEqual(
      expect.objectContaining({ lastSeq: 1, objects: [before?.object_data] }),
    );

    const valid: DurableCommand = {
      ...invalid,
      opId: crypto.randomUUID(),
      payload: { fill: "#ffffff" },
    };
    const committed = await repository.commitOperation(fixtureIds.user, valid);
    expect(committed.operation.seq).toBe(2);
    const retrieved = boardSnapshotSchema.parse(
      await repository.getBoard(boardId, fixtureIds.user),
    );
    expect(retrieved).toMatchObject({
      lastSeq: 2,
      objects: [{ id: create.targetId, kind: "rectangle", fill: "#ffffff", text: "" }],
    });
  });
});
