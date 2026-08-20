import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";

const read = (path: string): string => readFileSync(resolve(process.cwd(), path), "utf8");

describe("layered studio shell contract", () => {
  const workspace = read("src/components/workspace.tsx");
  const styles = read("app/styles.css");

  it("wraps the existing workspace in one isolated, labeled full-viewport shell", () => {
    expect(workspace).toContain(
      '<main className="workspace studio-shell" aria-label="Converge studio">',
    );
    expect(workspace).toContain('aria-label="Board header"');
    expect(workspace).toContain('id="studio-canvas-region"');
    expect(workspace).toContain('aria-label="Board canvas"');
    expect(styles).toMatch(/\.workspace\s*{[^}]*height:\s*100dvh;[^}]*isolation:\s*isolate;/s);
    expect(styles).toMatch(
      /\.studio-canvas-region\s*{[^}]*z-index:\s*var\(--layer-canvas,\s*100\);/s,
    );
  });

  it("uses only the existing canvas tools with correct native semantics", () => {
    expect(workspace).toContain('aria-label="Select tool"');
    expect(workspace).toContain('aria-label="Pan tool"');
    expect(workspace).toContain('aria-pressed={tool === "select"}');
    expect(workspace).toContain('aria-pressed={tool === "pan"}');
    expect(workspace).toContain('data-testid="add-rectangle"');
    expect(workspace).toContain('data-testid="add-sticky"');
    expect(workspace).toContain('aria-label="Delete selected"');
    expect(workspace).toContain('addObject("rectangle")');
    expect(workspace).toContain('addObject("sticky")');
    expect(workspace).not.toContain("undo");
    expect(workspace).not.toContain("redo");
  });

  it("keeps compact shared rotation controls fenced to the current eligible selection", () => {
    expect(workspace).toContain("<RotationControls");
    expect(workspace).toContain("rotationAvailable && selectedObject");
    expect(workspace).toContain("rotationControlsHadFocus");
    expect(workspace).toContain("layersTrigger.current?.focus()");
    expect(workspace).toContain(
      'rotationFence={`${store.sessionGeneration ?? "none"}:${store.connection}`}',
    );
  });

  it("keeps home, synchronization, terminal feedback, and diagnostics reachable", () => {
    expect(workspace).toContain('href="/" aria-label="Converge home"');
    expect(workspace).toContain("WorkspaceEntryStatus");
    expect(workspace).toContain('role="alert"');
    expect(workspace).toContain("aria-expanded={diagnostics}");
    expect(workspace).toContain('aria-controls="workspace-diagnostics-panel"');
    expect(workspace).toContain('event.key === "Escape" && diagnostics');
    expect(workspace).toContain("useState(false)");
    expect(workspace).toContain('<dl id="workspace-diagnostics-panel" hidden={!diagnostics}>');
  });

  it("keeps layers as a controlled local panel above the canvas without changing canvas geometry", () => {
    expect(workspace).toContain("<LayersPanel");
    expect(workspace).toContain("objects={store.objects}");
    expect(workspace).toContain("selectedId={store.selectedId}");
    expect(workspace).toContain("onSelect={(id) => store.select(id)}");
    expect(workspace).toContain('aria-controls="layers-panel"');
    expect(workspace).toContain('event.key === "Escape" && layersOpen');
    expect(styles).toMatch(/\.layers-panel\s*{[^}]*z-index:\s*var\(--layer-panels,\s*600\);/s);
    expect(styles).toMatch(
      /@media \(max-width: 600px\)\s*{[\s\S]*\.layers-panel\s*{[^}]*display:\s*none;/s,
    );
  });

  it("keeps overlay gaps transparent and presents an honest narrow-screen notice", () => {
    expect(styles).toMatch(/\.portal-hosts,\s*\.portal-root\s*{[^}]*pointer-events:\s*none;/s);
    expect(styles).toMatch(/\.studio-narrow-notice\s*{[^}]*display:\s*none;/s);
    expect(styles).toMatch(
      /@media \(max-width: 600px\)\s*{[\s\S]*\.studio-narrow-notice\s*{[^}]*pointer-events:\s*none;/s,
    );
    expect(workspace).toContain("Desktop-first studio");
    expect(workspace).toContain("optimized for a larger screen");
    expect(workspace).toContain("Restore local layers");
    expect(workspace).toContain("store.clearLocalViewControls()");
  });
});
