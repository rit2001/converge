import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { BoardRecoveryService, buildApp, type AppContext } from "@converge/api";
import type { AuthenticatedPrincipal } from "@converge/api/auth";
import { parseEnvironment } from "@converge/api/env";
import {
  BoardRecoveryMaterialRepository,
  BoardRecoveryError,
  BoardRecoveryRefreshInfrastructureError,
  BoardRepository,
  BoardSnapshotRepository,
  createPool,
  type RecoveryOperationEvidence,
} from "@converge/database";
import {
  createRectangleCommand,
  fixtureIds,
  TestAuthAdapter,
  testAuthorizationHeaders,
} from "@converge/testkit";
import {
  boardRecoveryMaterialSchema,
  boardSnapshotSchema,
  httpInternalErrorResponseSchema,
  operationRangeResponseSchema,
} from "@converge/protocol";
import type { DurableCommand } from "@converge/protocol";

const sharedDatabaseUrl =
  process.env.DATABASE_URL ?? "postgresql://converge:converge@127.0.0.1:55432/converge";
const repositoryRoot = new URL("../..", import.meta.url);
const runFile = promisify(execFile);
const boardIds = new Set<string>();
let databaseName: string;
let adminPool: ReturnType<typeof createPool>;
let pool: ReturnType<typeof createPool>;
let boards: BoardRepository;
let snapshots: BoardSnapshotRepository;
let recovery: BoardRecoveryMaterialRepository;
let api: AppContext;
const recoveryToken = "snapshot-recovery-owner-token";

function deferred(): { promise: Promise<void>; resolve(): void } {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

async function board(label: string): Promise<string> {
  const id = (await boards.createBoard(fixtureIds.user, `${label}-${crypto.randomUUID()}`)).id;
  boardIds.add(id);
  return id;
}

function command(
  boardId: string,
  baseSeq: number,
  type: DurableCommand["type"],
  targetId: string,
  payload: DurableCommand["payload"],
): DurableCommand {
  return {
    schemaVersion: 1,
    opId: crypto.randomUUID(),
    boardId,
    clientId: fixtureIds.clientA,
    baseSeq,
    type,
    targetId,
    payload,
    clientTimestamp: new Date().toISOString(),
  } as DurableCommand;
}

async function corruptHash(snapshotId: string): Promise<void> {
  await pool.query("ALTER TABLE board_snapshots DISABLE TRIGGER board_snapshots_immutable");
  try {
    await pool.query("UPDATE board_snapshots SET canonical_hash = $2 WHERE id = $1", [
      snapshotId,
      "f".repeat(64),
    ]);
  } finally {
    await pool.query("ALTER TABLE board_snapshots ENABLE TRIGGER board_snapshots_immutable");
  }
}

async function logicalBoardState(boardId: string): Promise<unknown> {
  const result = await pool.query<{ state: unknown }>(
    `SELECT jsonb_build_object(
      'board', (SELECT to_jsonb(b) FROM boards b WHERE id = $1),
      'objects', (SELECT jsonb_agg(to_jsonb(o) ORDER BY stack_order) FROM board_objects o WHERE board_id = $1),
      'operations', (SELECT jsonb_agg(to_jsonb(op) ORDER BY seq) FROM board_operations op WHERE board_id = $1),
      'outbox', (SELECT jsonb_agg(to_jsonb(e) ORDER BY delivery_seq) FROM outbox_events e WHERE board_id = $1),
      'members', (SELECT jsonb_agg(to_jsonb(m) ORDER BY user_id) FROM board_members m WHERE board_id = $1)
    ) AS state`,
    [boardId],
  );
  return result.rows[0]?.state;
}

beforeAll(async () => {
  databaseName = `converge_recovery_${process.pid}_${crypto
    .randomUUID()
    .replaceAll("-", "")
    .slice(0, 12)}`;
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
  recovery = new BoardRecoveryMaterialRepository(pool);
  api = await buildApp(
    parseEnvironment({
      NODE_ENV: "test",
      HOST: "127.0.0.1",
      API_PORT: "4000",
      WEB_ORIGIN: "http://127.0.0.1:3000",
      DATABASE_URL: isolatedUrl.toString(),
      REDIS_URL: "redis://127.0.0.1:6379",
      LOG_LEVEL: "silent",
      DEV_AUTH_USER_NAME: "Unused",
    }),
    pool,
    new TestAuthAdapter(
      new Map<string, AuthenticatedPrincipal>([
        [recoveryToken, { id: fixtureIds.user, displayName: "Recovery owner" }],
      ]),
    ),
  );
  const migration = await pool.query<{ name: string }>(
    "SELECT name FROM converge_migrations ORDER BY name DESC LIMIT 1",
  );
  expect(migration.rows[0]?.name).toBe("0008_snapshot_invalidation_diagnostics.sql");
});

afterEach(async () => {
  if (boardIds.size > 0)
    await pool.query("DELETE FROM boards WHERE id = ANY($1::uuid[])", [[...boardIds]]);
  boardIds.clear();
});

afterAll(async () => {
  await api?.app.close();
  await pool?.end();
  try {
    if (adminPool && databaseName) await adminPool.query(`DROP DATABASE "${databaseName}"`);
  } finally {
    await adminPool?.end();
  }
});

describe("snapshot plus contiguous operation-tail recovery", () => {
  it("exposes exact-head recovery without changing existing snapshot and range routes", async () => {
    const boardId = await board("api-exact-head");
    const snapshot = await snapshots.create(boardId);
    const headers = testAuthorizationHeaders(recoveryToken);
    const before = await pool.query(
      "SELECT last_seq, last_delivery_seq FROM boards WHERE id = $1",
      [boardId],
    );
    const logicalBefore = await logicalBoardState(boardId);
    const response = await api.app.inject({
      method: "GET",
      url: `/v1/boards/${boardId}/recovery`,
      headers,
    });
    expect(response.statusCode).toBe(200);
    expect(boardRecoveryMaterialSchema.parse(response.json())).toMatchObject({
      boardId,
      snapshotId: snapshot.id,
      snapshotCanvasSeq: 0,
      capturedCanvasSeq: 0,
      operationTail: [],
    });
    const existingSnapshot = await api.app.inject({
      method: "GET",
      url: `/v1/boards/${boardId}`,
      headers,
    });
    expect(existingSnapshot.statusCode).toBe(200);
    expect(boardSnapshotSchema.parse(existingSnapshot.json()).id).toBe(boardId);
    const existingRange = await api.app.inject({
      method: "GET",
      url: `/v1/boards/${boardId}/operations?after=0&watermark=0`,
      headers,
    });
    expect(existingRange.statusCode).toBe(200);
    expect(operationRangeResponseSchema.parse(existingRange.json()).operations).toEqual([]);
    const after = await pool.query("SELECT last_seq, last_delivery_seq FROM boards WHERE id = $1", [
      boardId,
    ]);
    expect(after.rows).toEqual(before.rows);
    expect(await logicalBoardState(boardId)).toEqual(logicalBefore);
  });

  it("serializes the actual earlier fallback snapshot, ordered state, tail, and hashes", async () => {
    const boardId = await board("api-fallback");
    const rectangleId = crypto.randomUUID();
    const stickyId = crypto.randomUUID();
    await boards.commitOperation(
      fixtureIds.user,
      createRectangleCommand(boardId, rectangleId, crypto.randomUUID()),
    );
    await boards.commitOperation(
      fixtureIds.user,
      command(boardId, 1, "object.create", stickyId, {
        id: stickyId,
        kind: "sticky",
        x: 20,
        y: 30,
        width: 120,
        height: 90,
        rotation: 12,
        fill: "#abcdef",
        text: "recovery",
      }),
    );
    await boards.commitOperation(
      fixtureIds.user,
      command(boardId, 2, "object.transform", rectangleId, { rotation: 43 }),
    );
    const earlier = await snapshots.create(boardId);
    await boards.commitOperation(
      fixtureIds.user,
      command(boardId, 3, "object.update", stickyId, { text: "tail" }),
    );
    const corrupt = await snapshots.create(boardId);
    await corruptHash(corrupt.id);
    const response = await api.app.inject({
      method: "GET",
      url: `/v1/boards/${boardId}/recovery`,
      headers: testAuthorizationHeaders(recoveryToken),
    });
    expect(response.statusCode).toBe(200);
    const body = boardRecoveryMaterialSchema.parse(response.json());
    expect(body.snapshotId).toBe(earlier.id);
    expect(body.snapshotState.objects.map(({ objectId }) => objectId)).toEqual([
      rectangleId,
      stickyId,
    ]);
    expect(body.snapshotState.objects[0]?.value.rotation).toBe(43);
    expect(body.operationTail.map(({ seq }) => seq)).toEqual([4]);
    const evidence = await recovery.load(boardId);
    expect(body.snapshotCanonicalHash).toBe(evidence.snapshotHash);
    expect(body.reconstructedCanonicalHash).toBe(evidence.reconstructedHash);
  });

  it("bootstraps missing recovery material once without advancing board heads", async () => {
    const boardId = await board("api-bootstrap");
    const logicalBefore = await logicalBoardState(boardId);
    const before = await pool.query(
      "SELECT last_seq, last_delivery_seq FROM boards WHERE id = $1",
      [boardId],
    );
    const response = await api.app.inject({
      method: "GET",
      url: `/v1/boards/${boardId}/recovery`,
      headers: testAuthorizationHeaders(recoveryToken),
    });
    expect(response.statusCode).toBe(200);
    const material = boardRecoveryMaterialSchema.parse(response.json());
    expect(material).toMatchObject({
      boardId,
      snapshotCanvasSeq: 0,
      snapshotDeliverySeq: 0,
      capturedCanvasSeq: 0,
      capturedDeliverySeq: 0,
      operationTail: [],
    });
    const snapshotCount = await pool.query<{ count: string }>(
      "SELECT count(*)::text AS count FROM board_snapshots WHERE board_id = $1",
      [boardId],
    );
    expect(snapshotCount.rows[0]?.count).toBe("1");
    const repeated = await api.app.inject({
      method: "GET",
      url: `/v1/boards/${boardId}/recovery`,
      headers: testAuthorizationHeaders(recoveryToken),
    });
    expect(boardRecoveryMaterialSchema.parse(repeated.json()).snapshotId).toBe(material.snapshotId);
    expect(
      (
        await pool.query<{ count: string }>(
          "SELECT count(*)::text AS count FROM board_snapshots WHERE board_id = $1",
          [boardId],
        )
      ).rows[0]?.count,
    ).toBe("1");
    const after = await pool.query("SELECT last_seq, last_delivery_seq FROM boards WHERE id = $1", [
      boardId,
    ]);
    expect(after.rows).toEqual(before.rows);
    expect(await logicalBoardState(boardId)).toEqual(logicalBefore);
  });

  it("refreshes a genesis snapshot and accepts it with an empty tail", async () => {
    const boardId = await board("genesis");
    await expect(recovery.load(boardId)).rejects.toMatchObject({
      code: "MISSING_REQUIRED_SNAPSHOT",
    });
    const snapshot = await recovery.refresh(boardId);
    const first = await recovery.load(boardId);
    const second = await recovery.load(boardId);
    expect(first).toEqual(second);
    expect(first.snapshotId).toBe(snapshot.snapshotId);
    expect(first.operations).toEqual([]);
    expect(first.capturedCanvasSeq).toBe(0);
  });

  it("uses a 100-operation tail and refreshes once at 101", async () => {
    const boardId = await board("tail-boundary");
    const initial = await snapshots.create(boardId);
    for (let sequence = 0; sequence < 100; sequence += 1)
      await boards.commitOperation(fixtureIds.user, createRectangleCommand(boardId));
    const headers = testAuthorizationHeaders(recoveryToken);
    const bounded = await api.app.inject({
      method: "GET",
      url: `/v1/boards/${boardId}/recovery`,
      headers,
    });
    expect(bounded.statusCode).toBe(200);
    const boundedMaterial = boardRecoveryMaterialSchema.parse(bounded.json());
    expect(boundedMaterial.snapshotId).toBe(initial.id);
    expect(boundedMaterial.operationTail).toHaveLength(100);
    expect(
      (
        await pool.query<{ count: string }>(
          "SELECT count(*)::text AS count FROM board_snapshots WHERE board_id = $1",
          [boardId],
        )
      ).rows[0]?.count,
    ).toBe("1");

    await boards.commitOperation(fixtureIds.user, createRectangleCommand(boardId));
    const headsBefore = await pool.query(
      "SELECT last_seq, last_delivery_seq FROM boards WHERE id = $1",
      [boardId],
    );
    const refreshed = await api.app.inject({
      method: "GET",
      url: `/v1/boards/${boardId}/recovery`,
      headers,
    });
    expect(refreshed.statusCode).toBe(200);
    const refreshedMaterial = boardRecoveryMaterialSchema.parse(refreshed.json());
    expect(refreshedMaterial.snapshotId).not.toBe(initial.id);
    expect(refreshedMaterial.snapshotCanvasSeq).toBe(101);
    expect(refreshedMaterial.capturedCanvasSeq).toBe(101);
    expect(refreshedMaterial.operationTail).toEqual([]);
    const headsAfter = await pool.query(
      "SELECT last_seq, last_delivery_seq FROM boards WHERE id = $1",
      [boardId],
    );
    expect(headsAfter.rows).toEqual(headsBefore.rows);
    expect(
      (
        await pool.query<{ count: string }>(
          "SELECT count(*)::text AS count FROM board_snapshots WHERE board_id = $1",
          [boardId],
        )
      ).rows[0]?.count,
    ).toBe("2");
  });

  it("uses a nonblocking refresh lock and creates at most one concurrent snapshot", async () => {
    const boardId = await board("concurrent-refresh");
    const entered = deferred();
    const release = deferred();
    let lockWinner = true;
    const reader = new BoardRecoveryMaterialRepository(pool, {
      hooks: {
        afterAdvisoryLock: async () => {
          if (!lockWinner) return;
          lockWinner = false;
          entered.resolve();
          await release.promise;
        },
      },
    });
    const first = reader.refresh(boardId);
    await entered.promise;
    await expect(reader.refresh(boardId)).rejects.toBeInstanceOf(
      BoardRecoveryRefreshInfrastructureError,
    );
    release.resolve();
    const material = await first;
    expect(material.operations).toEqual([]);
    const repeated = await reader.refresh(boardId);
    expect(repeated.snapshotId).toBe(material.snapshotId);
    expect(
      (
        await pool.query<{ count: string }>(
          "SELECT count(*)::text AS count FROM board_snapshots WHERE board_id = $1",
          [boardId],
        )
      ).rows[0]?.count,
    ).toBe("1");
  });

  it("maps busy and timed-out refresh infrastructure to retryable INTERNAL_ERROR", async () => {
    const auth = new TestAuthAdapter(
      new Map([[recoveryToken, { id: fixtureIds.user, displayName: "Recovery owner" }]]),
    );
    for (const mode of ["busy", "timeout"] as const) {
      const boardId = await board(`refresh-${mode}`);
      const blocker = await pool.connect();
      await blocker.query("BEGIN");
      if (mode === "busy")
        await blocker.query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", [boardId]);
      else await blocker.query("SELECT id FROM boards WHERE id = $1 FOR UPDATE", [boardId]);
      const refreshRepository = new BoardRecoveryMaterialRepository(pool, {
        refreshTimeoutMs: 25,
      });
      const service = new BoardRecoveryService({
        load: () => Promise.reject(new BoardRecoveryError("MISSING_REQUIRED_SNAPSHOT")),
        refresh: (id) => refreshRepository.refresh(id),
      });
      const context = await buildApp(
        parseEnvironment({
          NODE_ENV: "test",
          HOST: "127.0.0.1",
          API_PORT: "4000",
          WEB_ORIGIN: "http://127.0.0.1:3000",
          DATABASE_URL: sharedDatabaseUrl,
          REDIS_URL: "redis://127.0.0.1:6379",
          LOG_LEVEL: "silent",
          DEV_AUTH_USER_NAME: "Unused",
        }),
        pool,
        auth,
        { recoveryMaterialRepository: service },
      );
      try {
        const response = await context.app.inject({
          method: "GET",
          url: `/v1/boards/${boardId}/recovery`,
          headers: testAuthorizationHeaders(recoveryToken),
        });
        expect(response.statusCode).toBe(500);
        expect(httpInternalErrorResponseSchema.parse(response.json())).toMatchObject({
          code: "INTERNAL_ERROR",
          retryable: true,
        });
        expect(
          (
            await pool.query<{ count: string }>(
              "SELECT count(*)::text AS count FROM board_snapshots WHERE board_id = $1",
              [boardId],
            )
          ).rows[0]?.count,
        ).toBe("0");
      } finally {
        await context.app.close();
        await blocker.query("ROLLBACK");
        blocker.release();
      }
    }
  });

  it("keeps oversized and corrupt recovery refresh terminal without replacement", async () => {
    const oversizedBoard = await board("oversized-refresh");
    const oversized = new BoardRecoveryMaterialRepository(pool, { maximumSnapshotBytes: 1 });
    await expect(oversized.refresh(oversizedBoard)).rejects.toMatchObject({
      code: "SNAPSHOT_TOO_LARGE",
    });
    expect(
      (
        await pool.query<{ count: string }>(
          "SELECT count(*)::text AS count FROM board_snapshots WHERE board_id = $1",
          [oversizedBoard],
        )
      ).rows[0]?.count,
    ).toBe("0");
    const oversizedService = new BoardRecoveryService({
      load: () => Promise.reject(new BoardRecoveryError("MISSING_REQUIRED_SNAPSHOT")),
      refresh: (id) => oversized.refresh(id),
    });
    const oversizedContext = await buildApp(
      parseEnvironment({
        NODE_ENV: "test",
        HOST: "127.0.0.1",
        API_PORT: "4000",
        WEB_ORIGIN: "http://127.0.0.1:3000",
        DATABASE_URL: sharedDatabaseUrl,
        REDIS_URL: "redis://127.0.0.1:6379",
        LOG_LEVEL: "silent",
        DEV_AUTH_USER_NAME: "Unused",
      }),
      pool,
      new TestAuthAdapter(
        new Map([[recoveryToken, { id: fixtureIds.user, displayName: "Recovery owner" }]]),
      ),
      { recoveryMaterialRepository: oversizedService },
    );
    try {
      const oversizedResponse = await oversizedContext.app.inject({
        method: "GET",
        url: `/v1/boards/${oversizedBoard}/recovery`,
        headers: testAuthorizationHeaders(recoveryToken),
      });
      expect(oversizedResponse.statusCode).toBe(409);
      expect(oversizedResponse.json()).toMatchObject({
        code: "RECOVERY_BLOCKED",
        retryable: false,
      });
    } finally {
      await oversizedContext.app.close();
    }

    const corruptBoard = await board("corrupt-no-chain");
    const corrupt = await snapshots.create(corruptBoard);
    await corruptHash(corrupt.id);
    const response = await api.app.inject({
      method: "GET",
      url: `/v1/boards/${corruptBoard}/recovery`,
      headers: testAuthorizationHeaders(recoveryToken),
    });
    expect(response.statusCode).toBe(409);
    expect(response.json()).toMatchObject({ code: "RECOVERY_BLOCKED", retryable: false });
    const rows = await pool.query<{ status: string }>(
      "SELECT status FROM board_snapshots WHERE board_id = $1",
      [corruptBoard],
    );
    expect(rows.rows).toEqual([{ status: "invalid" }]);
  });

  it("reconstructs one and multiple ordered operations with stacking and rotation", async () => {
    const boardId = await board("tail");
    const snapshot = await snapshots.create(boardId);
    const firstId = crypto.randomUUID();
    const secondId = crypto.randomUUID();
    await boards.commitOperation(
      fixtureIds.user,
      createRectangleCommand(boardId, firstId, crypto.randomUUID()),
    );
    await boards.commitOperation(
      fixtureIds.user,
      command(boardId, 1, "object.create", secondId, {
        id: secondId,
        kind: "sticky",
        x: 10,
        y: 20,
        width: 100,
        height: 80,
        rotation: 13,
        fill: "#abcdef",
        text: "tail",
      }),
    );
    await boards.commitOperation(
      fixtureIds.user,
      command(boardId, 2, "object.transform", firstId, { rotation: 37 }),
    );
    const material = await recovery.load(boardId);
    expect(material.snapshotId).toBe(snapshot.id);
    expect(material.operations.map(({ seq }) => seq)).toEqual([1, 2, 3]);
    expect(material.reconstructedState.order).toEqual([firstId, secondId]);
    expect(material.reconstructedState.objects[firstId]?.value.rotation).toBe(37);
    expect(material.reconstructedHash).toMatch(/^[0-9a-f]{64}$/);
  });

  it("captures delivery-only revocation advancement without fabricating a canvas operation", async () => {
    const boardId = await board("revocation");
    const target = crypto.randomUUID();
    await pool.query(
      "INSERT INTO board_members(board_id, user_id, role) VALUES ($1, $2, 'editor')",
      [boardId, target],
    );
    const snapshot = await snapshots.create(boardId);
    await boards.removeBoardMember(fixtureIds.user, boardId, target);
    const material = await recovery.load(boardId);
    expect(material.snapshotId).toBe(snapshot.id);
    expect(material.snapshotDeliverySeq).toBe(0);
    expect(material.capturedDeliverySeq).toBe(1);
    expect(material.operations).toEqual([]);
  });

  it("invalidates a corrupt newest snapshot and selects a complete earlier chain", async () => {
    const boardId = await board("fallback");
    const immutableBefore = await snapshots.create(boardId);
    await boards.commitOperation(fixtureIds.user, createRectangleCommand(boardId));
    const earlier = await snapshots.create(boardId);
    await boards.commitOperation(fixtureIds.user, createRectangleCommand(boardId));
    const corrupt = await snapshots.create(boardId);
    await corruptHash(corrupt.id);
    const immutableCorruptBefore = await pool.query(
      `SELECT id, board_id, snapshot_seq, snapshot_delivery_seq, schema_version, projection,
              canonical_hash, object_count, byte_size, created_at
       FROM board_snapshots WHERE id = $1`,
      [corrupt.id],
    );
    const headsBefore = await pool.query(
      "SELECT last_seq, last_delivery_seq FROM boards WHERE id = $1",
      [boardId],
    );

    const material = await recovery.load(boardId);
    expect(material.snapshotId).toBe(earlier.id);
    expect(material.operations.map(({ seq }) => seq)).toEqual([2]);
    expect(material.capturedCanvasSeq).toBe(2);
    const invalid = await pool.query<{
      status: string;
      invalidation_code: string;
      projection: unknown;
      board_id: string;
      snapshot_seq: string;
      snapshot_delivery_seq: string;
      created_at: Date;
    }>(
      `SELECT status, invalidation_code, projection, board_id, snapshot_seq,
              snapshot_delivery_seq, created_at
       FROM board_snapshots WHERE id = $1`,
      [corrupt.id],
    );
    expect(invalid.rows[0]).toMatchObject({
      status: "invalid",
      invalidation_code: "CORRUPT_SNAPSHOT",
      board_id: corrupt.boardId,
      snapshot_seq: String(corrupt.snapshotSeq),
      snapshot_delivery_seq: String(corrupt.snapshotDeliverySeq),
      projection: corrupt.projection,
    });
    expect(invalid.rows[0]?.invalidation_code).not.toContain(JSON.stringify(corrupt.projection));
    expect(invalid.rows[0]?.invalidation_code.length).toBeLessThanOrEqual(64);
    const immutableCorruptAfter = await pool.query(
      `SELECT id, board_id, snapshot_seq, snapshot_delivery_seq, schema_version, projection,
              canonical_hash, object_count, byte_size, created_at
       FROM board_snapshots WHERE id = $1`,
      [corrupt.id],
    );
    const headsAfter = await pool.query(
      "SELECT last_seq, last_delivery_seq FROM boards WHERE id = $1",
      [boardId],
    );
    expect(immutableCorruptAfter.rows).toEqual(immutableCorruptBefore.rows);
    expect(headsAfter.rows).toEqual(headsBefore.rows);
    const invalidatedAt = await pool.query<{ invalidated_at: Date }>(
      "SELECT invalidated_at FROM board_snapshots WHERE id = $1",
      [corrupt.id],
    );
    const again = await recovery.load(boardId);
    const invalidatedAgain = await pool.query<{ invalidated_at: Date }>(
      "SELECT invalidated_at FROM board_snapshots WHERE id = $1",
      [corrupt.id],
    );
    expect(again.snapshotId).toBe(earlier.id);
    expect(invalidatedAgain.rows[0]?.invalidated_at).toEqual(invalidatedAt.rows[0]?.invalidated_at);
    expect(immutableBefore.snapshotSeq).toBe(0);
  });

  it("durably invalidates corrupt newest then blocks an incomplete earlier tail", async () => {
    const boardId = await board("gap");
    await snapshots.create(boardId);
    await boards.commitOperation(fixtureIds.user, createRectangleCommand(boardId));
    await boards.commitOperation(fixtureIds.user, createRectangleCommand(boardId));
    const corrupt = await snapshots.create(boardId);
    await corruptHash(corrupt.id);
    await pool.query("DELETE FROM board_operations WHERE board_id = $1 AND seq = 1", [boardId]);
    await expect(recovery.load(boardId)).rejects.toMatchObject({ code: "SNAPSHOT_CORRUPT" });
    const status = await pool.query<{ status: string }>(
      "SELECT status FROM board_snapshots WHERE id = $1",
      [corrupt.id],
    );
    expect(status.rows[0]?.status).toBe("invalid");
  });

  it("blocks bounded fallback when the earlier tail exceeds the configured limit", async () => {
    const boardId = await board("limit");
    await snapshots.create(boardId);
    await boards.commitOperation(fixtureIds.user, createRectangleCommand(boardId));
    await boards.commitOperation(fixtureIds.user, createRectangleCommand(boardId));
    const corrupt = await snapshots.create(boardId);
    await corruptHash(corrupt.id);
    const bounded = new BoardRecoveryMaterialRepository(pool, { tailLimit: 1 });
    await expect(bounded.load(boardId)).rejects.toMatchObject({ code: "SNAPSHOT_CORRUPT" });
    expect(
      (
        await pool.query<{ status: string }>("SELECT status FROM board_snapshots WHERE id = $1", [
          corrupt.id,
        ])
      ).rows[0]?.status,
    ).toBe("invalid");
  });

  it("invalidates multiple corrupt candidates before selecting the valid earlier snapshot", async () => {
    const boardId = await board("multiple-corrupt");
    const valid = await snapshots.create(boardId);
    await boards.commitOperation(fixtureIds.user, createRectangleCommand(boardId));
    const corruptOne = await snapshots.create(boardId);
    await boards.commitOperation(fixtureIds.user, createRectangleCommand(boardId));
    const corruptTwo = await snapshots.create(boardId);
    await corruptHash(corruptOne.id);
    await corruptHash(corruptTwo.id);
    const material = await recovery.load(boardId);
    expect(material.snapshotId).toBe(valid.id);
    expect(material.operations).toHaveLength(2);
    const statuses = await pool.query<{ status: string }>(
      "SELECT status FROM board_snapshots WHERE id = ANY($1::uuid[]) ORDER BY snapshot_seq",
      [[corruptOne.id, corruptTwo.id]],
    );
    expect(statuses.rows.map(({ status }) => status)).toEqual(["invalid", "invalid"]);
  });

  it("blocks gaps, duplicates, wrong-board, malformed, and beyond-head tail evidence", async () => {
    const boardId = await board("tail-evidence");
    await snapshots.create(boardId);
    await boards.commitOperation(fixtureIds.user, createRectangleCommand(boardId));
    const cases: Array<{
      code: string;
      transform(rows: readonly RecoveryOperationEvidence[]): readonly RecoveryOperationEvidence[];
    }> = [
      { code: "TAIL_GAP", transform: () => [] },
      { code: "TAIL_ORDER_CONFLICT", transform: (rows) => [rows[0]!, rows[0]!] },
      {
        code: "WRONG_BOARD_OPERATION",
        transform: (rows) => [{ ...rows[0]!, board_id: crypto.randomUUID() }],
      },
      { code: "MALFORMED_OPERATION", transform: (rows) => [{ ...rows[0]!, command: {} }] },
      {
        code: "OPERATION_BEYOND_HEAD",
        transform: (rows) => [{ ...rows[0]!, seq: "2" }],
      },
    ];
    for (const testCase of cases) {
      const reader = new BoardRecoveryMaterialRepository(pool, {
        hooks: {
          transformTailEvidence: (rows) => Promise.resolve(testCase.transform(rows)),
        },
      });
      await expect(reader.load(boardId)).rejects.toMatchObject({ code: testCase.code });
    }
  });

  it("blocks reducer and authoritative projection mismatches", async () => {
    const reducerBoard = await board("reducer-mismatch");
    await snapshots.create(reducerBoard);
    await boards.commitOperation(fixtureIds.user, createRectangleCommand(reducerBoard));
    const reducerFailure = new BoardRecoveryMaterialRepository(pool, {
      hooks: {
        transformTailEvidence: (rows) => {
          const original = rows[0]!;
          return Promise.resolve([
            {
              ...original,
              command: {
                ...(original.command as object),
                type: "object.update",
                targetId: crypto.randomUUID(),
                payload: { fill: "#abcdef" },
              },
            },
          ]);
        },
      },
    });
    await expect(reducerFailure.load(reducerBoard)).rejects.toMatchObject({
      code: "REDUCER_FAILURE",
    });

    const projectionBoard = await board("projection-mismatch");
    await snapshots.create(projectionBoard);
    const operation = await boards.commitOperation(
      fixtureIds.user,
      createRectangleCommand(projectionBoard),
    );
    await pool.query(
      `UPDATE board_objects
       SET object_data = jsonb_set(object_data, '{fill}', '"#000000"'::jsonb)
       WHERE board_id = $1 AND object_id = $2`,
      [projectionBoard, operation.operation.targetId],
    );
    await expect(recovery.load(projectionBoard)).rejects.toMatchObject({
      code: "PROJECTION_MISMATCH",
    });
  });

  it("holds operation and revocation commits outside its fixed locked boundary", async () => {
    const operationBoard = await board("operation-boundary");
    await snapshots.create(operationBoard);
    const captured = deferred();
    const release = deferred();
    const reader = new BoardRecoveryMaterialRepository(pool, {
      hooks: {
        afterBoundaryCaptured: async () => {
          captured.resolve();
          await release.promise;
        },
      },
    });
    const loading = reader.load(operationBoard);
    await captured.promise;
    const committing = boards.commitOperation(
      fixtureIds.user,
      createRectangleCommand(operationBoard),
    );
    release.resolve();
    const [material, committed] = await Promise.all([loading, committing]);
    expect(material.capturedCanvasSeq).toBe(0);
    expect(committed.operation.seq).toBe(1);

    const revocationBoard = await board("revocation-boundary");
    const target = crypto.randomUUID();
    await pool.query(
      "INSERT INTO board_members(board_id, user_id, role) VALUES ($1, $2, 'editor')",
      [revocationBoard, target],
    );
    await snapshots.create(revocationBoard);
    const revocationCaptured = deferred();
    const revocationRelease = deferred();
    const revocationReader = new BoardRecoveryMaterialRepository(pool, {
      hooks: {
        afterBoundaryCaptured: async () => {
          revocationCaptured.resolve();
          await revocationRelease.promise;
        },
      },
    });
    const revocationLoading = revocationReader.load(revocationBoard);
    await revocationCaptured.promise;
    const removing = boards.removeBoardMember(fixtureIds.user, revocationBoard, target);
    revocationRelease.resolve();
    const [revocationMaterial, removed] = await Promise.all([revocationLoading, removing]);
    expect(revocationMaterial.capturedDeliverySeq).toBe(0);
    expect(removed.event?.deliverySeq).toBe(1);
  });

  it("changes no durable state during a healthy recovery read", async () => {
    const boardId = await board("read-only");
    await snapshots.create(boardId);
    await boards.commitOperation(fixtureIds.user, createRectangleCommand(boardId));
    const before = await pool.query<{ state: unknown }>(
      `SELECT jsonb_build_object(
        'board', (SELECT to_jsonb(b) FROM boards b WHERE id = $1),
        'objects', (SELECT jsonb_agg(to_jsonb(o) ORDER BY stack_order) FROM board_objects o WHERE board_id = $1),
        'operations', (SELECT jsonb_agg(to_jsonb(op) ORDER BY seq) FROM board_operations op WHERE board_id = $1),
        'outbox', (SELECT jsonb_agg(to_jsonb(e) ORDER BY delivery_seq) FROM outbox_events e WHERE board_id = $1),
        'members', (SELECT jsonb_agg(to_jsonb(m) ORDER BY user_id) FROM board_members m WHERE board_id = $1),
        'snapshots', (SELECT jsonb_agg(to_jsonb(s) ORDER BY snapshot_seq) FROM board_snapshots s WHERE board_id = $1)
      ) AS state`,
      [boardId],
    );
    await recovery.load(boardId);
    const after = await pool.query<{ state: unknown }>(
      `SELECT jsonb_build_object(
        'board', (SELECT to_jsonb(b) FROM boards b WHERE id = $1),
        'objects', (SELECT jsonb_agg(to_jsonb(o) ORDER BY stack_order) FROM board_objects o WHERE board_id = $1),
        'operations', (SELECT jsonb_agg(to_jsonb(op) ORDER BY seq) FROM board_operations op WHERE board_id = $1),
        'outbox', (SELECT jsonb_agg(to_jsonb(e) ORDER BY delivery_seq) FROM outbox_events e WHERE board_id = $1),
        'members', (SELECT jsonb_agg(to_jsonb(m) ORDER BY user_id) FROM board_members m WHERE board_id = $1),
        'snapshots', (SELECT jsonb_agg(to_jsonb(s) ORDER BY snapshot_seq) FROM board_snapshots s WHERE board_id = $1)
      ) AS state`,
      [boardId],
    );
    expect(after.rows[0]?.state).toEqual(before.rows[0]?.state);
  });
});
