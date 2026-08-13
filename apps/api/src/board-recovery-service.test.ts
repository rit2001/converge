import { describe, expect, it, vi } from "vitest";
import {
  BoardRecoveryError,
  BoardRecoveryRefreshInfrastructureError,
  type VerifiedBoardRecoveryMaterial,
} from "@converge/database";
import { BoardRecoveryService } from "./board-recovery-service.js";

const boardId = "20000000-0000-4000-8000-000000000081";
const material = { boardId } as VerifiedBoardRecoveryMaterial;

describe("board recovery refresh service", () => {
  it("reports whether material was loaded directly or refreshed", async () => {
    const direct = new BoardRecoveryService({
      load: () => Promise.resolve(material),
      refresh: () => Promise.resolve(material),
    });
    const refreshed = new BoardRecoveryService({
      load: () => Promise.reject(new BoardRecoveryError("MISSING_REQUIRED_SNAPSHOT")),
      refresh: () => Promise.resolve(material),
    });

    await expect(direct.loadWithOutcome(boardId)).resolves.toEqual({
      material,
      outcome: "snapshot_tail",
    });
    await expect(refreshed.loadWithOutcome(boardId)).resolves.toEqual({
      material,
      outcome: "refreshed",
    });
  });

  it("returns normal material without creating a snapshot", async () => {
    const load = vi.fn(() => Promise.resolve(material));
    const refresh = vi.fn(() => Promise.resolve(material));
    const service = new BoardRecoveryService({ load, refresh });

    await expect(service.load(boardId)).resolves.toBe(material);
    expect(load).toHaveBeenCalledExactlyOnceWith(boardId);
    expect(refresh).not.toHaveBeenCalled();
  });

  it.each(["MISSING_REQUIRED_SNAPSHOT", "TAIL_LIMIT_EXCEEDED"] as const)(
    "performs one refresh for %s",
    async (code) => {
      const refresh = vi.fn(() => Promise.resolve(material));
      const service = new BoardRecoveryService({
        load: () => Promise.reject(new BoardRecoveryError(code)),
        refresh,
      });

      await expect(service.load(boardId)).resolves.toBe(material);
      expect(refresh).toHaveBeenCalledExactlyOnceWith(boardId);
    },
  );

  it.each([
    "SNAPSHOT_TOO_LARGE",
    "SNAPSHOT_BELOW_RECOVERY_FLOOR",
    "SNAPSHOT_CORRUPT",
    "UNSUPPORTED_SNAPSHOT_VERSION",
    "TAIL_GAP",
    "TAIL_ORDER_CONFLICT",
    "WRONG_BOARD_OPERATION",
    "MALFORMED_OPERATION",
    "OPERATION_BEYOND_HEAD",
    "REDUCER_FAILURE",
    "PROJECTION_MISMATCH",
    "CANONICAL_HASH_MISMATCH",
    "NO_COMPLETE_RECOVERY_CHAIN",
  ] as const)("does not refresh durable %s evidence", async (code) => {
    const refresh = vi.fn(() => Promise.resolve(material));
    const service = new BoardRecoveryService({
      load: () => Promise.reject(new BoardRecoveryError(code)),
      refresh,
    });

    await expect(service.load(boardId)).rejects.toMatchObject({ code });
    expect(refresh).not.toHaveBeenCalled();
  });

  it("does not loop when the single refresh attempt fails", async () => {
    const refreshFailure = new BoardRecoveryError("TAIL_LIMIT_EXCEEDED");
    const refresh = vi.fn(() => Promise.reject(refreshFailure));
    const service = new BoardRecoveryService({
      load: () => Promise.reject(new BoardRecoveryError("MISSING_REQUIRED_SNAPSHOT")),
      refresh,
    });

    await expect(service.load(boardId)).rejects.toBe(refreshFailure);
    expect(refresh).toHaveBeenCalledOnce();
  });

  it("preserves retryable infrastructure failures from refresh", async () => {
    const infrastructure = new BoardRecoveryRefreshInfrastructureError("BOARD_LOCK_BUSY");
    const service = new BoardRecoveryService({
      load: () => Promise.reject(new BoardRecoveryError("MISSING_REQUIRED_SNAPSHOT")),
      refresh: () => Promise.reject(infrastructure),
    });

    await expect(service.load(boardId)).rejects.toBe(infrastructure);
  });
});
