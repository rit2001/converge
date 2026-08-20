import { describe, expect, it } from "vitest";
import { deriveSynchronizationPresentation } from "./synchronization-presentation";
import type { SynchronizationPresentationInput } from "./synchronization-presentation";

const current = (
  overrides: Partial<SynchronizationPresentationInput> = {},
): SynchronizationPresentationInput => ({
  hasCurrentSession: true,
  hasBoard: true,
  connection: "ready",
  pendingCount: 0,
  pendingStatus: "idle",
  ...overrides,
});

describe("synchronization presentation", () => {
  it.each([
    [{ connection: "connecting" }, "connecting"],
    [{ connection: "joining" }, "restoring"],
    [{ connection: "catching-up" }, "restoring"],
    [{ connection: "ready", pendingCount: 1 }, "saving"],
    [{ connection: "retry-wait" }, "reconnecting"],
    [{ connection: "disconnected", pendingCount: 1 }, "locally_preserved"],
    [{ connection: "authorization-failed", pendingCount: 1 }, "access_removed"],
    [{ connection: "error", pendingCount: 1 }, "recovery_blocked"],
    [{ hasCurrentSession: false }, "unavailable"],
    [{ connection: "ready", pendingStatus: "pending-retry", pendingCount: 0 }, "unavailable"],
  ] satisfies Array<[Partial<SynchronizationPresentationInput>, string]>)(
    "maps %# fail-closed",
    (input, state) => {
      expect(deriveSynchronizationPresentation(current(input)).state).toBe(state);
    },
  );

  it("allows synced only for a current ready session with no pending commands", () => {
    expect(deriveSynchronizationPresentation(current()).state).toBe("synced");
    expect(deriveSynchronizationPresentation(current({ pendingCount: 1 })).state).toBe("saving");
    expect(
      deriveSynchronizationPresentation(current({ connection: "disconnected", pendingCount: 1 }))
        .state,
    ).toBe("locally_preserved");
  });

  it("gives terminal and stale evidence precedence over ready and pending evidence", () => {
    expect(
      deriveSynchronizationPresentation(
        current({ connection: "authorization-failed", pendingCount: 2 }),
      ).state,
    ).toBe("access_removed");
    expect(
      deriveSynchronizationPresentation(current({ connection: "error", pendingCount: 2 })).state,
    ).toBe("recovery_blocked");
    expect(
      deriveSynchronizationPresentation(current({ hasBoard: false, pendingCount: 2 })).state,
    ).toBe("unavailable");
  });
});
