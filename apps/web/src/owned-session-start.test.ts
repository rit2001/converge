import { describe, expect, it, vi } from "vitest";
import { scheduleOwnedSessionStart } from "./owned-session-start";

function controlledMicrotasks(): {
  schedule: (callback: () => void) => void;
  flush: () => void;
} {
  const callbacks: Array<() => void> = [];
  return {
    schedule: (callback) => callbacks.push(callback),
    flush: () => callbacks.splice(0).forEach((callback) => callback()),
  };
}

describe("owned session start", () => {
  it("does not initialize a mount discarded before its microtask boundary", () => {
    const microtasks = controlledMicrotasks();
    const start = vi.fn(() => ({ generation: 1 }));
    const stop = vi.fn();

    const cleanup = scheduleOwnedSessionStart(start, stop, microtasks.schedule);
    cleanup();
    microtasks.flush();

    expect(start).not.toHaveBeenCalled();
    expect(stop).not.toHaveBeenCalled();
  });

  it("starts and stops the surviving mount exactly once", () => {
    const microtasks = controlledMicrotasks();
    const handle = { generation: 2 };
    const start = vi.fn(() => handle);
    const stop = vi.fn();

    const cleanup = scheduleOwnedSessionStart(start, stop, microtasks.schedule);
    microtasks.flush();
    cleanup();
    cleanup();

    expect(start).toHaveBeenCalledTimes(1);
    expect(stop).toHaveBeenCalledTimes(1);
    expect(stop).toHaveBeenCalledWith(handle);
  });

  it("collapses development replay to the one surviving session", () => {
    const microtasks = controlledMicrotasks();
    const start = vi.fn(() => ({ generation: 1 }));
    const stop = vi.fn();

    const discardedCleanup = scheduleOwnedSessionStart(start, stop, microtasks.schedule);
    discardedCleanup();
    const survivingCleanup = scheduleOwnedSessionStart(start, stop, microtasks.schedule);
    microtasks.flush();

    expect(start).toHaveBeenCalledTimes(1);
    survivingCleanup();
    expect(stop).toHaveBeenCalledTimes(1);
  });
});
