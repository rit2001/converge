import { describe, expect, it, vi } from "vitest";
import {
  COMMAND_IDS,
  createWorkspaceCommands,
  MAX_COMMAND_QUERY_LENGTH,
  searchCommands,
} from "./commands";

function commands() {
  const action = COMMAND_IDS.reduce<Record<(typeof COMMAND_IDS)[number], () => void>>(
    (result, id) => ({ ...result, [id]: vi.fn() }),
    {} as Record<(typeof COMMAND_IDS)[number], () => void>,
  );
  return createWorkspaceCommands({
    ready: true,
    hasSelection: true,
    selectedLocked: false,
    selectedHidden: false,
    rotateAvailable: true,
    canvasAvailable: true,
    mutationAllowed: true,
    action,
  });
}

describe("workspace command registry", () => {
  it("has one stable implemented command catalog without speculative features", () => {
    const catalog = commands();
    expect(catalog.map((command) => command.id)).toEqual(COMMAND_IDS);
    expect(catalog.map((command) => command.label).join(" ")).not.toMatch(/undo|history|group/i);
  });
  it("ranks exact, label prefix, keyword prefix, and substring matches deterministically", () => {
    const catalog = commands();
    expect(searchCommands(catalog, "zoom in")[0]?.id).toBe("view.zoom-in");
    expect(
      searchCommands(catalog, "zoom")
        .map((command) => command.id)
        .slice(0, 3),
    ).toEqual(["view.zoom-in", "view.zoom-out", "view.reset-zoom"]);
    expect(searchCommands(catalog, "hand")[0]?.id).toBe("tool.pan");
    expect(searchCommands(catalog, "clock")[0]?.id).toBe("selection.rotate-clockwise");
  });
  it("bounds queries/results and gives unavailable selection commands a fixed reason", () => {
    const action = COMMAND_IDS.reduce<Record<(typeof COMMAND_IDS)[number], () => void>>(
      (result, id) => ({ ...result, [id]: vi.fn() }),
      {} as Record<(typeof COMMAND_IDS)[number], () => void>,
    );
    const catalog = createWorkspaceCommands({
      ready: false,
      hasSelection: false,
      selectedLocked: false,
      selectedHidden: false,
      rotateAvailable: false,
      canvasAvailable: false,
      mutationAllowed: false,
      action,
    });
    expect(catalog.find((command) => command.id === "selection.delete")).toMatchObject({
      available: false,
      disabledReason: "Board is not ready for editing.",
    });
    expect(searchCommands(catalog, "x".repeat(MAX_COMMAND_QUERY_LENGTH + 20))).toEqual([]);
  });
  it("keeps direct keyboard creation and canvas focus in the bounded catalog", () => {
    const catalog = commands();
    expect(catalog.find((command) => command.id === "view.focus-canvas")?.available).toBe(true);
    expect(searchCommands(catalog, "viewport center").map((command) => command.id)).toEqual([
      "create.rectangle-center",
      "create.sticky-center",
    ]);
  });
});
