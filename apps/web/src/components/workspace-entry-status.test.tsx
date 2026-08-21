import * as React from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";
import type { SynchronizationStatus } from "../board-store";
import { WorkspaceEntryStatus, workspaceStatePresentation } from "./workspace-entry-status";

describe("workspace entry status", () => {
  it.each([
    ["disconnected", false, "Preparing", false],
    ["disconnected", true, "Unavailable", false],
    ["connecting", false, "Connecting", false],
    ["joining", true, "Synchronizing", false],
    ["catching-up", true, "Catching up", false],
    ["retry-wait", true, "Recovering", false],
    ["authorization-failed", true, "Access revoked", true],
    ["error", true, "Recovery needs attention", true],
  ] satisfies Array<[SynchronizationStatus, boolean, string, boolean]>)(
    "maps %s directly from the existing synchronization state",
    (status, hasBoard, label, terminal) => {
      expect(workspaceStatePresentation(status, hasBoard)).toMatchObject({ label, terminal });
    },
  );

  it("does not render a second ready state over the workspace", () => {
    expect(workspaceStatePresentation("ready", true)).toBeNull();
    expect(renderToStaticMarkup(<WorkspaceEntryStatus status="ready" hasBoard />)).toBe("");
  });

  it("reserves the canvas-covering entry surface for terminal states", () => {
    const recovering = renderToStaticMarkup(<WorkspaceEntryStatus status="retry-wait" hasBoard />);
    const terminal = renderToStaticMarkup(
      <WorkspaceEntryStatus status="authorization-failed" hasBoard />,
    );

    expect(recovering).toBe("");
    expect(terminal).toContain('aria-live="assertive"');
    expect(terminal).toContain('role="alert"');
    expect(terminal).toContain('href="/"');
    expect(terminal).toContain("Return home");
  });
});
