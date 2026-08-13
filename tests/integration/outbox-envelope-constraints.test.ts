import { afterAll, beforeAll, describe, expect, it } from "vitest";
import type { PoolClient } from "pg";
import { createPool } from "@converge/database";
import {
  membershipRevokedDeliveryEnvelopeSchema,
  operationCommittedDeliveryEnvelopeSchema,
} from "@converge/protocol";
import { createRectangleCommand, fixtureIds } from "@converge/testkit";

const databaseUrl =
  process.env.DATABASE_URL ?? "postgresql://converge:converge@localhost:55432/converge";
const pool = createPool(databaseUrl);

interface OutboxInsert {
  id: string;
  boardId: string;
  deliverySeq: number;
  canvasSeq: number | null;
  eventType: "operation.committed" | "board.membership.revoked";
  schemaVersion: number;
  payload: Record<string, unknown>;
}

beforeAll(async () => {
  await pool.query("SELECT 1");
});

afterAll(async () => {
  await pool.end();
});

function operationInsert(boardId: string, deliverySeq: number): OutboxInsert {
  const eventId = crypto.randomUUID();
  const occurredAt = "2026-08-09T12:00:00.000Z";
  const operation = {
    ...createRectangleCommand(boardId),
    seq: deliverySeq,
    committedAt: occurredAt,
  };
  return {
    id: eventId,
    boardId,
    deliverySeq,
    canvasSeq: deliverySeq,
    eventType: "operation.committed",
    schemaVersion: 1,
    payload: operationCommittedDeliveryEnvelopeSchema.parse({
      schemaVersion: 1,
      eventId,
      boardId,
      deliverySeq,
      eventType: "operation.committed",
      occurredAt,
      payload: { operation },
    }),
  };
}

function revocationInsert(boardId: string, deliverySeq: number): OutboxInsert {
  const eventId = crypto.randomUUID();
  return {
    id: eventId,
    boardId,
    deliverySeq,
    canvasSeq: null,
    eventType: "board.membership.revoked",
    schemaVersion: 1,
    payload: membershipRevokedDeliveryEnvelopeSchema.parse({
      schemaVersion: 1,
      eventId,
      boardId,
      deliverySeq,
      eventType: "board.membership.revoked",
      occurredAt: "2026-08-09T12:00:01.000Z",
      payload: {
        revokedUserId: "70000000-0000-4000-8000-000000000001",
        initiatedByUserId: fixtureIds.user,
      },
    }),
  };
}

async function insertOutbox(client: PoolClient, insert: OutboxInsert): Promise<void> {
  await client.query(
    `INSERT INTO outbox_events(
       id, board_id, delivery_seq, canvas_seq, event_type, schema_version, payload
     ) VALUES ($1,$2,$3,$4,$5,$6,$7)`,
    [
      insert.id,
      insert.boardId,
      insert.deliverySeq,
      insert.canvasSeq,
      insert.eventType,
      insert.schemaVersion,
      insert.payload,
    ],
  );
}

async function expectCheckRejected(client: PoolClient, insert: OutboxInsert): Promise<void> {
  await client.query("SAVEPOINT invalid_envelope");
  await expect(insertOutbox(client, insert)).rejects.toMatchObject({ code: "23514" });
  await client.query("ROLLBACK TO SAVEPOINT invalid_envelope");
  await client.query("RELEASE SAVEPOINT invalid_envelope");
}

async function withBoard(
  run: (client: PoolClient, boardId: string) => Promise<void>,
): Promise<void> {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const boardId = crypto.randomUUID();
    await client.query("INSERT INTO boards(id, name, created_by) VALUES ($1, $2, $3)", [
      boardId,
      "outbox-envelope-constraint",
      fixtureIds.user,
    ]);
    await run(client, boardId);
  } finally {
    await client.query("ROLLBACK");
    client.release();
  }
}

function clonePayload(insert: OutboxInsert): Record<string, unknown> {
  return structuredClone(insert.payload);
}

describe("complete outbox envelope constraints", () => {
  it("accepts valid operation and membership-revocation envelopes", async () => {
    await withBoard(async (client, boardId) => {
      await expect(insertOutbox(client, operationInsert(boardId, 1))).resolves.toBeUndefined();
      await expect(insertOutbox(client, revocationInsert(boardId, 2))).resolves.toBeUndefined();
    });
  });

  it("rejects every missing, null, or incorrectly typed envelope metadata field", async () => {
    await withBoard(async (client, boardId) => {
      const requiredFields = [
        "schemaVersion",
        "eventId",
        "boardId",
        "deliverySeq",
        "eventType",
        "occurredAt",
        "payload",
      ] as const;
      const wrongTypes: Record<(typeof requiredFields)[number], unknown> = {
        schemaVersion: "1",
        eventId: 1,
        boardId: 1,
        deliverySeq: "1",
        eventType: 1,
        occurredAt: 1,
        payload: [],
      };
      let deliverySeq = 10;
      for (const field of requiredFields) {
        const missing = operationInsert(boardId, deliverySeq++);
        const missingPayload = clonePayload(missing);
        delete missingPayload[field];
        await expectCheckRejected(client, { ...missing, payload: missingPayload });

        const explicitNull = operationInsert(boardId, deliverySeq++);
        await expectCheckRejected(client, {
          ...explicitNull,
          payload: { ...clonePayload(explicitNull), [field]: null },
        });

        const wrongType = operationInsert(boardId, deliverySeq++);
        await expectCheckRejected(client, {
          ...wrongType,
          payload: { ...clonePayload(wrongType), [field]: wrongTypes[field] },
        });
      }

      const malformedTimestamp = operationInsert(boardId, deliverySeq);
      await expectCheckRejected(client, {
        ...malformedTimestamp,
        payload: { ...clonePayload(malformedTimestamp), occurredAt: "not-a-timestamp" },
      });
    });
  });

  it("rejects envelope metadata that disagrees with relational columns", async () => {
    await withBoard(async (client, boardId) => {
      const mismatches: Array<[string, unknown]> = [
        ["schemaVersion", 2],
        ["eventId", crypto.randomUUID()],
        ["boardId", crypto.randomUUID()],
        ["deliverySeq", 9_999],
        ["eventType", "board.membership.revoked"],
      ];
      let deliverySeq = 100;
      for (const [field, value] of mismatches) {
        const insert = operationInsert(boardId, deliverySeq++);
        await expectCheckRejected(client, {
          ...insert,
          payload: { ...clonePayload(insert), [field]: value },
        });
      }
    });
  });

  it("rejects missing, null, mismatched, or invalid operation payload identity", async () => {
    await withBoard(async (client, boardId) => {
      let deliverySeq = 200;
      for (const mutate of [
        (payload: Record<string, unknown>) => {
          delete payload.operation;
        },
        (payload: Record<string, unknown>) => {
          payload.operation = null;
        },
        (payload: Record<string, unknown>) => {
          const operation = payload.operation as Record<string, unknown>;
          delete operation.boardId;
        },
        (payload: Record<string, unknown>) => {
          const operation = payload.operation as Record<string, unknown>;
          operation.boardId = null;
        },
        (payload: Record<string, unknown>) => {
          const operation = payload.operation as Record<string, unknown>;
          operation.boardId = crypto.randomUUID();
        },
        (payload: Record<string, unknown>) => {
          const operation = payload.operation as Record<string, unknown>;
          delete operation.seq;
        },
        (payload: Record<string, unknown>) => {
          const operation = payload.operation as Record<string, unknown>;
          operation.seq = null;
        },
        (payload: Record<string, unknown>) => {
          const operation = payload.operation as Record<string, unknown>;
          operation.seq = 9_999;
        },
      ]) {
        const insert = operationInsert(boardId, deliverySeq++);
        const envelope = clonePayload(insert);
        const variantPayload = structuredClone(envelope.payload) as Record<string, unknown>;
        mutate(variantPayload);
        envelope.payload = variantPayload;
        await expectCheckRejected(client, { ...insert, payload: envelope });
      }
    });
  });

  it("rejects missing, null, or invalid membership-revocation principals", async () => {
    await withBoard(async (client, boardId) => {
      let deliverySeq = 300;
      for (const field of ["revokedUserId", "initiatedByUserId"] as const) {
        for (const invalid of [undefined, null, 42, "not-a-uuid"]) {
          const insert = revocationInsert(boardId, deliverySeq++);
          const envelope = clonePayload(insert);
          const variantPayload = structuredClone(envelope.payload) as Record<string, unknown>;
          if (invalid === undefined) delete variantPayload[field];
          else variantPayload[field] = invalid;
          envelope.payload = variantPayload;
          await expectCheckRejected(client, { ...insert, payload: envelope });
        }
      }
    });
  });
});
