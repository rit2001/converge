import * as React from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it, vi } from "vitest";
import type { CanvasObject } from "@converge/protocol";
import { LayersPanel, layerEntries, nextLayerFocus } from "./layers-panel";

const rectangle = (id: string): CanvasObject => ({
  id,
  kind: "rectangle",
  x: 0,
  y: 0,
  width: 120,
  height: 80,
  rotation: 0,
  fill: "#818cf8",
  text: "",
});
const sticky = (id: string): CanvasObject => ({ ...rectangle(id), kind: "sticky", text: "Note" });

describe("LayersPanel", () => {
  it("reads the authoritative render order from top to bottom", () => {
    expect(
      layerEntries([rectangle("bottom"), sticky("top")]).map((entry) => entry.object.id),
    ).toEqual(["top", "bottom"]);
  });

  it("adds deterministic suffixes only for duplicate object types", () => {
    expect(
      layerEntries([rectangle("a"), sticky("b"), rectangle("c")]).map((entry) => entry.label),
    ).toEqual(["Rectangle 1", "Sticky note", "Rectangle 2"]);
  });

  it("uses a semantic button list with independent focus and selected states", () => {
    const markup = renderToStaticMarkup(
      <LayersPanel
        objects={[rectangle("first"), sticky("second")]}
        selectedId="first"
        onSelect={vi.fn()}
        onClose={vi.fn()}
      />,
    );

    expect(markup).toContain('aria-label="Board layers, top to bottom"');
    expect(markup).toContain('aria-label="Sticky note, top layer, 1 of 2"');
    expect(markup).toContain('aria-label="Rectangle, bottom layer, 2 of 2"');
    expect(markup).toContain('aria-pressed="true"');
    expect(markup).toContain('aria-pressed="false"');
    expect(markup).toContain("ui-focus-ring");
  });

  it("handles keyboard focus navigation without implicitly selecting an object", () => {
    expect(nextLayerFocus(1, "ArrowUp", 3)).toBe(0);
    expect(nextLayerFocus(1, "ArrowDown", 3)).toBe(2);
    expect(nextLayerFocus(1, "Home", 3)).toBe(0);
    expect(nextLayerFocus(1, "End", 3)).toBe(2);
  });

  it("has an instructional empty state and accurate object count", () => {
    const markup = renderToStaticMarkup(
      <LayersPanel objects={[]} selectedId={null} onSelect={vi.fn()} onClose={vi.fn()} />,
    );

    expect(markup).toContain("0 objects");
    expect(markup).toContain("Your board is ready for its first idea.");
  });
});
