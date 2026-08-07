import pg from "pg";
import {
  emptyBoardState,
  reduceCommand,
  type BoardState,
  type ProjectedObject,
} from "@converge/canvas-engine";
import {
  canvasObjectSchema,
  committedOperationSchema,
  durableCommandSchema,
  type BoardSnapshot,
  type CommittedOperation,
  type DurableCommand,
} from "@converge/protocol";

export class RepositoryError extends Error {
  constructor(
    public readonly code:
      | "BOARD_NOT_FOUND"
      | "FORBIDDEN"
      | "INVALID_COMMAND"
      | "TARGET_NOT_FOUND"
      | "TARGET_DELETED"
      | "CONFLICT",
    message: string,
  ) {
    super(message);
  }
}

export function createPool(databaseUrl: string): pg.Pool {
  return new pg.Pool({ connectionString: databaseUrl, max: 20 });
}

export class BoardRepository {
  constructor(private readonly pool: pg.Pool) {}

  async createBoard(userId: string, name: string): Promise<BoardSnapshot> {
    const id = crypto.randomUUID();
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      await client.query("INSERT INTO boards(id, name, created_by) VALUES ($1, $2, $3)", [
        id,
        name,
        userId,
      ]);
      await client.query(
        "INSERT INTO board_members(board_id, user_id, role) VALUES ($1, $2, 'owner')",
        [id, userId],
      );
      await client.query("COMMIT");
      return { id, name, lastSeq: 0, objects: [] };
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }

  async roleFor(boardId: string, userId: string): Promise<string | null> {
    const result = await this.pool.query<{ role: string }>(
      "SELECT role FROM board_members WHERE board_id = $1 AND user_id = $2",
      [boardId, userId],
    );
    return result.rows[0]?.role ?? null;
  }

  async getBoard(boardId: string, userId: string): Promise<BoardSnapshot> {
    const result = await this.pool.query<{
      id: string;
      name: string;
      last_seq: string;
      object_data: unknown;
    }>(
      `SELECT b.id, b.name, b.last_seq, o.object_data
       FROM boards b JOIN board_members m ON m.board_id = b.id AND m.user_id = $2
       LEFT JOIN board_objects o ON o.board_id = b.id AND o.deleted_seq IS NULL
       WHERE b.id = $1 ORDER BY o.object_id`,
      [boardId, userId],
    );
    if (!result.rowCount) {
      const exists = await this.pool.query("SELECT 1 FROM boards WHERE id = $1", [boardId]);
      throw new RepositoryError(
        exists.rowCount ? "FORBIDDEN" : "BOARD_NOT_FOUND",
        exists.rowCount ? "Board membership required" : "Board not found",
      );
    }
    const first = result.rows[0];
    if (!first) throw new RepositoryError("BOARD_NOT_FOUND", "Board not found");
    return {
      id: first.id,
      name: first.name,
      lastSeq: Number(first.last_seq),
      objects: result.rows.flatMap((row) =>
        row.object_data ? [canvasObjectSchema.parse(row.object_data)] : [],
      ),
    };
  }

  async getOperations(
    boardId: string,
    userId: string,
    from: number,
    to: number,
  ): Promise<CommittedOperation[]> {
    if (!(await this.roleFor(boardId, userId)))
      throw new RepositoryError("FORBIDDEN", "Board membership required");
    const result = await this.pool.query<{ command: unknown; seq: string; committed_at: Date }>(
      "SELECT command, seq, committed_at FROM board_operations WHERE board_id = $1 AND seq BETWEEN $2 AND $3 ORDER BY seq",
      [boardId, from, to],
    );
    return result.rows.map((row) =>
      committedOperationSchema.parse({
        ...durableCommandSchema.parse(row.command),
        seq: Number(row.seq),
        committedAt: row.committed_at.toISOString(),
      }),
    );
  }

  async commitOperation(
    userId: string,
    input: DurableCommand,
  ): Promise<{ duplicate: boolean; operation: CommittedOperation }> {
    const command = durableCommandSchema.parse(input);
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      await client.query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", [
        command.boardId,
      ]);
      const role = await client.query<{ role: string }>(
        "SELECT role FROM board_members WHERE board_id = $1 AND user_id = $2",
        [command.boardId, userId],
      );
      if (!role.rowCount || role.rows[0]?.role === "viewer")
        throw new RepositoryError("FORBIDDEN", "Board edit permission required");
      const duplicate = await client.query<{ command: unknown; seq: string; committed_at: Date }>(
        "SELECT command, seq, committed_at FROM board_operations WHERE board_id = $1 AND op_id = $2",
        [command.boardId, command.opId],
      );
      const prior = duplicate.rows[0];
      if (prior) {
        await client.query("COMMIT");
        return {
          duplicate: true,
          operation: committedOperationSchema.parse({
            ...durableCommandSchema.parse(prior.command),
            seq: Number(prior.seq),
            committedAt: prior.committed_at.toISOString(),
          }),
        };
      }
      const board = await client.query<{ last_seq: string }>(
        "SELECT last_seq FROM boards WHERE id = $1 FOR UPDATE",
        [command.boardId],
      );
      if (!board.rowCount) throw new RepositoryError("BOARD_NOT_FOUND", "Board not found");
      const rows = await client.query<{
        object_id: string;
        object_data: unknown;
        field_seq: Record<string, number>;
        created_seq: string;
        updated_seq: string;
        deleted_seq: string | null;
      }>(
        "SELECT object_id, object_data, field_seq, created_seq, updated_seq, deleted_seq FROM board_objects WHERE board_id = $1",
        [command.boardId],
      );
      const state: BoardState = emptyBoardState();
      state.lastSeq = Number(board.rows[0]?.last_seq ?? 0);
      for (const row of rows.rows) {
        const projected: ProjectedObject = {
          value: canvasObjectSchema.parse(row.object_data),
          fieldSeq: row.field_seq,
          createdSeq: Number(row.created_seq),
          updatedSeq: Number(row.updated_seq),
          deletedSeq: row.deleted_seq === null ? null : Number(row.deleted_seq),
        };
        state.objects[row.object_id] = projected;
        if (projected.deletedSeq === null) state.order.push(row.object_id);
      }
      state.order.sort();
      const seq = state.lastSeq + 1;
      const reduced = reduceCommand(state, command, seq);
      if (!reduced.ok) throw new RepositoryError(reduced.code, reduced.message);
      const projected = reduced.state.objects[command.targetId];
      if (!projected) throw new RepositoryError("CONFLICT", "Projection update failed");
      const committedAt = new Date().toISOString();
      const operation = committedOperationSchema.parse({ ...command, seq, committedAt });
      await client.query(
        `INSERT INTO board_operations(board_id, seq, op_id, client_id, user_id, base_seq, type, target_id, command, committed_at)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
        [
          command.boardId,
          seq,
          command.opId,
          command.clientId,
          userId,
          command.baseSeq,
          command.type,
          command.targetId,
          command,
          committedAt,
        ],
      );
      await client.query(
        `INSERT INTO board_objects(board_id, object_id, kind, object_data, field_seq, created_seq, updated_seq, deleted_seq)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
         ON CONFLICT (board_id, object_id) DO UPDATE SET object_data=EXCLUDED.object_data, field_seq=EXCLUDED.field_seq,
           updated_seq=EXCLUDED.updated_seq, deleted_seq=EXCLUDED.deleted_seq`,
        [
          command.boardId,
          command.targetId,
          projected.value.kind,
          projected.value,
          projected.fieldSeq,
          projected.createdSeq,
          projected.updatedSeq,
          projected.deletedSeq,
        ],
      );
      await client.query("UPDATE boards SET last_seq = $2, updated_at = now() WHERE id = $1", [
        command.boardId,
        seq,
      ]);
      await client.query(
        "INSERT INTO outbox_events(id, board_id, board_seq, event_type, payload) VALUES ($1,$2,$3,'operation.committed',$4)",
        [crypto.randomUUID(), command.boardId, seq, operation],
      );
      await client.query("COMMIT");
      return { duplicate: false, operation };
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }
}

export type DatabasePool = pg.Pool;
