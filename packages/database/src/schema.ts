import {
  bigint,
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

export const boards = pgTable("boards", {
  id: uuid("id").primaryKey(),
  name: text("name").notNull(),
  createdBy: uuid("created_by").notNull(),
  lastSeq: bigint("last_seq", { mode: "number" }).notNull().default(0),
  createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
  updatedAt: timestamp("updated_at", { withTimezone: true }).notNull().defaultNow(),
});

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
    updatedSeq: bigint("updated_seq", { mode: "number" }).notNull(),
    deletedSeq: bigint("deleted_seq", { mode: "number" }),
  },
  (table) => [
    primaryKey({ columns: [table.boardId, table.objectId] }),
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
    baseSeq: bigint("base_seq", { mode: "number" }).notNull(),
    type: text("type").notNull(),
    targetId: uuid("target_id").notNull(),
    command: jsonb("command").notNull(),
    committedAt: timestamp("committed_at", { withTimezone: true }).notNull().defaultNow(),
  },
  (table) => [
    primaryKey({ columns: [table.boardId, table.seq] }),
    uniqueIndex("board_operations_board_op_id_uq").on(table.boardId, table.opId),
  ],
);

export const outboxEvents = pgTable(
  "outbox_events",
  {
    id: uuid("id").primaryKey(),
    boardId: uuid("board_id").notNull(),
    boardSeq: bigint("board_seq", { mode: "number" }).notNull(),
    eventType: text("event_type").notNull(),
    payload: jsonb("payload").notNull(),
    attempts: integer("attempts").notNull().default(0),
    createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
    publishedAt: timestamp("published_at", { withTimezone: true }),
  },
  (table) => [
    uniqueIndex("outbox_board_seq_uq").on(table.boardId, table.boardSeq),
    index("outbox_unpublished_idx").on(table.publishedAt, table.createdAt),
  ],
);
