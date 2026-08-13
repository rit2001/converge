import {
  bigint,
  check,
  index,
  integer,
  jsonb,
  pgTable,
  primaryKey,
  text,
  timestamp,
  uniqueIndex,
  uuid,
} from "drizzle-orm/pg-core";
import { sql } from "drizzle-orm";

export const boards = pgTable(
  "boards",
  {
    id: uuid("id").primaryKey(),
    name: text("name").notNull(),
    createdBy: uuid("created_by").notNull(),
    lastSeq: bigint("last_seq", { mode: "number" }).notNull().default(0),
    lastDeliverySeq: bigint("last_delivery_seq", { mode: "number" }).notNull().default(0),
    operationRecoveryFloor: bigint("operation_recovery_floor", { mode: "number" })
      .notNull()
      .default(0),
    deliveryRecoveryFloor: bigint("delivery_recovery_floor", { mode: "number" })
      .notNull()
      .default(0),
    createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
    updatedAt: timestamp("updated_at", { withTimezone: true }).notNull().defaultNow(),
  },
  (table) => [
    check("boards_last_delivery_seq_check", sql`${table.lastDeliverySeq} >= 0`),
    check(
      "boards_delivery_head_covers_canvas_check",
      sql`${table.lastDeliverySeq} >= ${table.lastSeq}`,
    ),
    check(
      "boards_operation_recovery_floor_check",
      sql`${table.operationRecoveryFloor} BETWEEN 0 AND 9007199254740991
          AND ${table.operationRecoveryFloor} <= ${table.lastSeq}`,
    ),
    check(
      "boards_delivery_recovery_floor_check",
      sql`${table.deliveryRecoveryFloor} BETWEEN 0 AND 9007199254740991
          AND ${table.deliveryRecoveryFloor} <= ${table.lastDeliverySeq}`,
    ),
  ],
);

export const boardMembers = pgTable(
  "board_members",
  {
    boardId: uuid("board_id")
      .notNull()
      .references(() => boards.id, { onDelete: "cascade" }),
    userId: uuid("user_id").notNull(),
    role: text("role").notNull(),
    createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
  },
  (table) => [primaryKey({ columns: [table.boardId, table.userId] })],
);

export const boardObjects = pgTable(
  "board_objects",
  {
    boardId: uuid("board_id")
      .notNull()
      .references(() => boards.id, { onDelete: "cascade" }),
    objectId: uuid("object_id").notNull(),
    kind: text("kind").notNull(),
    objectData: jsonb("object_data").notNull(),
    fieldSeq: jsonb("field_seq").notNull(),
    createdSeq: bigint("created_seq", { mode: "number" }).notNull(),
    stackOrder: bigint("stack_order", { mode: "number" }).notNull(),
    updatedSeq: bigint("updated_seq", { mode: "number" }).notNull(),
    deletedSeq: bigint("deleted_seq", { mode: "number" }),
  },
  (table) => [
    primaryKey({ columns: [table.boardId, table.objectId] }),
    uniqueIndex("board_objects_stack_order_uq").on(table.boardId, table.stackOrder),
    index("board_objects_active_idx").on(table.boardId, table.deletedSeq),
  ],
);

export const boardOperations = pgTable(
  "board_operations",
  {
    boardId: uuid("board_id")
      .notNull()
      .references(() => boards.id, { onDelete: "cascade" }),
    seq: bigint("seq", { mode: "number" }).notNull(),
    opId: uuid("op_id").notNull(),
    clientId: uuid("client_id").notNull(),
    userId: uuid("user_id").notNull(),
    eventId: uuid("event_id").notNull(),
    deliverySeq: bigint("delivery_seq", { mode: "number" }).notNull(),
    baseSeq: bigint("base_seq", { mode: "number" }).notNull(),
    type: text("type").notNull(),
    targetId: uuid("target_id").notNull(),
    command: jsonb("command").notNull(),
    committedAt: timestamp("committed_at", { withTimezone: true }).notNull().defaultNow(),
  },
  (table) => [
    primaryKey({ columns: [table.boardId, table.seq] }),
    uniqueIndex("board_operations_board_op_id_uq").on(table.boardId, table.opId),
    uniqueIndex("board_operations_event_id_uq").on(table.eventId),
    uniqueIndex("board_operations_board_delivery_seq_uq").on(table.boardId, table.deliverySeq),
  ],
);

export const boardOperationReceipts = pgTable(
  "board_operation_receipts",
  {
    boardId: uuid("board_id")
      .notNull()
      .references(() => boards.id, { onDelete: "cascade" }),
    operationId: uuid("operation_id").notNull(),
    actorId: uuid("actor_id").notNull(),
    command: jsonb("command").notNull(),
    commandHash: text("command_hash").notNull(),
    hashSchemaVersion: integer("hash_schema_version").notNull(),
    canvasSeq: bigint("canvas_seq", { mode: "number" }).notNull(),
    deliverySeq: bigint("delivery_seq", { mode: "number" }).notNull(),
    eventId: uuid("event_id").notNull(),
    committedAt: timestamp("committed_at", { withTimezone: true }).notNull(),
    result: jsonb("result").notNull(),
  },
  (table) => [
    primaryKey({ columns: [table.boardId, table.operationId] }),
    uniqueIndex("board_operation_receipts_board_canvas_seq_uq").on(table.boardId, table.canvasSeq),
    uniqueIndex("board_operation_receipts_board_delivery_seq_uq").on(
      table.boardId,
      table.deliverySeq,
    ),
    uniqueIndex("board_operation_receipts_event_id_uq").on(table.eventId),
    check(
      "board_operation_receipts_canvas_seq_check",
      sql`${table.canvasSeq} BETWEEN 1 AND 9007199254740991`,
    ),
    check(
      "board_operation_receipts_delivery_seq_check",
      sql`${table.deliverySeq} BETWEEN ${table.canvasSeq} AND 9007199254740991`,
    ),
    check(
      "board_operation_receipts_command_hash_check",
      sql`${table.commandHash} ~ '^[0-9a-f]{64}$'`,
    ),
    check(
      "board_operation_receipts_hash_schema_version_check",
      sql`${table.hashSchemaVersion} = 1`,
    ),
    check(
      "board_operation_receipts_consistency_check",
      sql`(converge_operation_receipt_is_valid(
        ${table.command},
        ${table.result},
        ${table.boardId},
        ${table.operationId},
        ${table.canvasSeq},
        ${table.deliverySeq},
        ${table.eventId},
        ${table.committedAt},
        ${table.hashSchemaVersion},
        ${table.commandHash}
      )) IS TRUE`,
    ),
  ],
);

export const boardSnapshots = pgTable(
  "board_snapshots",
  {
    id: uuid("id").primaryKey(),
    boardId: uuid("board_id")
      .notNull()
      .references(() => boards.id, { onDelete: "cascade" }),
    snapshotSeq: bigint("snapshot_seq", { mode: "number" }).notNull(),
    snapshotDeliverySeq: bigint("snapshot_delivery_seq", { mode: "number" }).notNull(),
    schemaVersion: integer("schema_version").notNull(),
    projection: jsonb("projection").notNull(),
    canonicalHash: text("canonical_hash").notNull(),
    objectCount: integer("object_count").notNull(),
    byteSize: integer("byte_size").notNull(),
    status: text("status").notNull(),
    createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
    verifiedAt: timestamp("verified_at", { withTimezone: true }),
    invalidationCode: text("invalidation_code"),
    invalidatedAt: timestamp("invalidated_at", { withTimezone: true }),
  },
  (table) => [
    uniqueIndex("board_snapshots_board_seq_uq").on(table.boardId, table.snapshotSeq),
    index("board_snapshots_latest_verified_idx").on(table.boardId, table.status, table.snapshotSeq),
    check("board_snapshots_snapshot_seq_check", sql`${table.snapshotSeq} >= 0`),
    check(
      "board_snapshots_delivery_seq_check",
      sql`${table.snapshotDeliverySeq} >= ${table.snapshotSeq}`,
    ),
    check("board_snapshots_schema_version_check", sql`${table.schemaVersion} > 0`),
    check("board_snapshots_object_count_check", sql`${table.objectCount} >= 0`),
    check("board_snapshots_byte_size_check", sql`${table.byteSize} BETWEEN 1 AND 16777216`),
    check(
      "board_snapshots_status_check",
      sql`${table.status} IN ('creating', 'verified', 'invalid')`,
    ),
    check(
      "board_snapshots_invalidation_state_check",
      sql`((
        ${table.status} IN ('creating', 'verified')
        AND ${table.invalidationCode} IS NULL
        AND ${table.invalidatedAt} IS NULL
      ) OR (
        ${table.status} = 'invalid'
        AND ${table.invalidationCode} ~ '^[A-Z][A-Z0-9_]{0,63}$'
        AND ${table.invalidatedAt} IS NOT NULL
      )) IS TRUE`,
    ),
  ],
);

export const outboxEvents = pgTable(
  "outbox_events",
  {
    id: uuid("id").primaryKey(),
    boardId: uuid("board_id")
      .notNull()
      .references(() => boards.id, { onDelete: "cascade" }),
    deliverySeq: bigint("delivery_seq", { mode: "number" }).notNull(),
    canvasSeq: bigint("canvas_seq", { mode: "number" }),
    eventType: text("event_type").notNull(),
    schemaVersion: integer("schema_version").notNull(),
    payload: jsonb("payload").notNull(),
    status: text("status").notNull().default("pending"),
    attemptCount: integer("attempt_count").notNull().default(0),
    leaseOwner: text("lease_owner"),
    leaseToken: uuid("lease_token"),
    leasedUntil: timestamp("leased_until", { withTimezone: true }),
    nextAttemptAt: timestamp("next_attempt_at", { withTimezone: true }).notNull().defaultNow(),
    redisEntryId: text("redis_entry_id"),
    lastErrorCode: text("last_error_code"),
    lastErrorMessage: text("last_error_message"),
    lastErrorAt: timestamp("last_error_at", { withTimezone: true }),
    createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
    updatedAt: timestamp("updated_at", { withTimezone: true }).notNull().defaultNow(),
    publishedAt: timestamp("published_at", { withTimezone: true }),
  },
  (table) => [
    uniqueIndex("outbox_board_delivery_seq_uq").on(table.boardId, table.deliverySeq),
    uniqueIndex("outbox_lease_token_uq")
      .on(table.leaseToken)
      .where(sql`${table.leaseToken} IS NOT NULL`),
    uniqueIndex("outbox_operation_canvas_seq_uq")
      .on(table.boardId, table.canvasSeq)
      .where(sql`${table.eventType} = 'operation.committed'`),
    check("outbox_delivery_seq_check", sql`${table.deliverySeq} > 0`),
    check("outbox_schema_version_check", sql`${table.schemaVersion} = 1`),
    check(
      "outbox_status_check",
      sql`${table.status} IN ('pending', 'leased', 'retry_wait', 'published', 'blocked')`,
    ),
    check(
      "outbox_attempt_count_check",
      sql`${table.attemptCount} BETWEEN 0 AND 20
          AND (${table.status} NOT IN ('leased', 'retry_wait', 'blocked') OR ${table.attemptCount} > 0)
          AND (${table.status} NOT IN ('pending', 'retry_wait') OR ${table.attemptCount} < 20)`,
    ),
    check(
      "outbox_lease_state_check",
      sql`((
        ${table.status} = 'leased'
        AND ${table.leaseOwner} IS NOT NULL
        AND ${table.leaseToken} IS NOT NULL
        AND ${table.leasedUntil} IS NOT NULL
      ) OR (
        ${table.status} <> 'leased'
        AND ${table.leaseOwner} IS NULL
        AND ${table.leaseToken} IS NULL
        AND ${table.leasedUntil} IS NULL
      )) IS TRUE`,
    ),
    check(
      "outbox_publication_state_check",
      sql`((
        ${table.status} = 'published'
        AND ${table.publishedAt} IS NOT NULL
        AND ${table.redisEntryId} IS NOT NULL
      ) OR (
        ${table.status} <> 'published'
        AND ${table.publishedAt} IS NULL
        AND ${table.redisEntryId} IS NULL
      )) IS TRUE`,
    ),
    check(
      "outbox_event_canvas_seq_check",
      sql`(${table.eventType} = 'operation.committed' AND ${table.canvasSeq} IS NOT NULL AND ${table.canvasSeq} > 0)
          OR (${table.eventType} = 'board.membership.revoked' AND ${table.canvasSeq} IS NULL)`,
    ),
    index("outbox_eligible_idx")
      .on(table.nextAttemptAt, table.createdAt, table.id)
      .where(sql`${table.status} IN ('pending', 'retry_wait')`),
    index("outbox_lease_recovery_idx")
      .on(table.leasedUntil, table.createdAt, table.id)
      .where(sql`${table.status} = 'leased'`),
    index("outbox_board_predecessor_idx").on(table.boardId, table.deliverySeq, table.status),
    index("outbox_status_created_idx").on(table.status, table.createdAt),
    index("outbox_published_at_idx")
      .on(table.publishedAt)
      .where(sql`${table.status} = 'published'`),
  ],
);
