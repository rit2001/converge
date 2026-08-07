import { describe, expect, it, vi } from "vitest";
import type { CommittedOperation } from "@converge/protocol";
import { deliverCommittedOperation } from "./app.js";

const operation: CommittedOperation = {
  schemaVersion: 1,
  opId: "40000000-0000-4000-8000-000000000001",
  boardId: "10000000-0000-4000-8000-000000000001",
  clientId: "20000000-0000-4000-8000-000000000001",
  baseSeq: 0,
  targetId: "30000000-0000-4000-8000-000000000001",
  clientTimestamp: "2026-08-07T12:00:00.000Z",
  type: "object.create",
  payload: {
    id: "30000000-0000-4000-8000-000000000001",
    kind: "rectangle",
    x: 0,
    y: 0,
    width: 100,
    height: 80,
    rotation: 0,
    fill: "#818cf8",
    text: "",
  },
  seq: 1,
  committedAt: "2026-08-07T12:00:01.000Z",
};

describe("post-commit operation delivery", () => {
  it("publishes before acknowledgement and isolates acknowledgement failure", () => {
    const calls: string[] = [];
    const publish = vi.fn(() => calls.push("publish"));
    const acknowledge = vi.fn(() => {
      calls.push("acknowledge");
      throw new Error("transport acknowledgement failed");
    });
    const reportFailure = vi.fn((stage: string) => calls.push(`report:${stage}`));

    expect(() =>
      deliverCommittedOperation({
        operation,
        duplicate: false,
        publish,
        acknowledge,
        reportFailure,
      }),
    ).not.toThrow();
    expect(calls).toEqual(["publish", "acknowledge", "report:acknowledge"]);
    expect(publish).toHaveBeenCalledOnce();
    expect(acknowledge).toHaveBeenCalledWith({ ok: true, duplicate: false, operation });
  });

  it("attempts acknowledgement even if live publication fails", () => {
    const acknowledge = vi.fn();
    const reportFailure = vi.fn();

    expect(() =>
      deliverCommittedOperation({
        operation,
        duplicate: true,
        publish: () => {
          throw new Error("publication failed");
        },
        acknowledge,
        reportFailure,
      }),
    ).not.toThrow();
    expect(reportFailure).toHaveBeenCalledWith("publish", expect.any(Error));
    expect(acknowledge).toHaveBeenCalledWith({ ok: true, duplicate: true, operation });
  });
});
