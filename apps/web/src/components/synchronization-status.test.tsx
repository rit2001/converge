// @vitest-environment jsdom

import * as React from "react";
import { act } from "react";
import { createRoot } from "react-dom/client";
import { afterEach, describe, expect, it } from "vitest";
import type { SynchronizationPresentation } from "../synchronization-presentation";
import { SynchronizationStatus } from "./synchronization-status";

const reactTestEnvironment = globalThis as typeof globalThis & {
  IS_REACT_ACT_ENVIRONMENT: boolean;
};
reactTestEnvironment.IS_REACT_ACT_ENVIRONMENT = true;
let root: ReturnType<typeof createRoot> | null = null;
const synced: SynchronizationPresentation = {
  state: "synced",
  label: "Synced",
  tone: "success",
  readyForEditing: true,
  pendingPreservedLocally: false,
  terminal: false,
  next: "New edits can be sent to the board.",
};

afterEach(() => {
  void act(() => root?.unmount());
  root = null;
  document.body.replaceChildren();
});

describe("SynchronizationStatus", () => {
  it("opens details with native activation and restores focus after Escape or outside click", () => {
    const host = document.createElement("div");
    const portal = document.createElement("div");
    portal.id = "overlay-popovers";
    document.body.append(host, portal);
    root = createRoot(host);
    void act(() => root?.render(<SynchronizationStatus presentation={synced} pendingCount={0} />));
    const trigger = host.querySelector(
      'button[aria-label="Synchronization status: Synced"]',
    ) as HTMLButtonElement;
    expect(trigger.getAttribute("aria-expanded")).toBe("false");
    void act(() => trigger.click());
    expect(trigger.getAttribute("aria-expanded")).toBe("true");
    expect(portal.textContent).toContain("Pending changes0");
    void act(() =>
      window.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape", bubbles: true })),
    );
    expect(trigger.getAttribute("aria-expanded")).toBe("false");
    expect(document.activeElement).toBe(trigger);
    void act(() => trigger.click());
    void act(() => document.body.dispatchEvent(new MouseEvent("mousedown", { bubbles: true })));
    expect(trigger.getAttribute("aria-expanded")).toBe("false");
  });

  it("uses bounded semantic text rather than raw failure evidence", () => {
    const host = document.createElement("div");
    const portal = document.createElement("div");
    portal.id = "overlay-popovers";
    document.body.append(host, portal);
    root = createRoot(host);
    const terminal = {
      ...synced,
      state: "recovery_blocked" as const,
      label: "Recovery needs attention",
      tone: "danger" as const,
      readyForEditing: false,
      terminal: true,
      next: "Editing stays disabled because this board could not be safely recovered.",
    };
    void act(() =>
      root?.render(<SynchronizationStatus presentation={terminal} pendingCount={2} />),
    );
    const trigger = host.querySelector("button")!;
    void act(() => trigger.click());
    expect(portal.textContent).not.toMatch(/[0-9a-f]{8}-[0-9a-f-]{27}/i);
    expect(portal.textContent).not.toContain("Error:");
  });
});
