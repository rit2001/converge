import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";

const styles = readFileSync(resolve(process.cwd(), "app/styles.css"), "utf8");

const requiredTokens = [
  "--color-background-application",
  "--color-background-canvas",
  "--color-surface-primary",
  "--color-surface-secondary",
  "--color-surface-elevated",
  "--color-surface-floating",
  "--color-text-primary",
  "--color-text-secondary",
  "--color-text-muted",
  "--color-text-inverse",
  "--color-border-subtle",
  "--color-border-default",
  "--color-border-strong",
  "--color-border-focus",
  "--color-action-base",
  "--color-action-hover",
  "--color-action-pressed",
  "--color-action-subtle",
  "--color-selection",
  "--color-canvas-focus",
  "--color-success",
  "--color-warning",
  "--color-danger",
  "--color-information",
  "--color-reconnecting",
  "--color-recovering",
  "--color-unavailable",
  "--color-revoked",
  "--color-collaborator-1",
  "--color-collaborator-8",
  "--space-1",
  "--space-16",
  "--radius-small",
  "--radius-panel",
  "--shadow-control",
  "--shadow-modal",
  "--font-family-product",
  "--font-size-body",
  "--line-height-body",
  "--font-weight-semibold",
  "--icon-size-medium",
  "--control-height-default",
  "--motion-duration-micro",
  "--motion-duration-emphasis",
  "--motion-easing-standard",
  "--focus-ring-width",
  "--focus-ring-offset",
  "--focus-ring-color",
  "--color-canvas-grid-dot",
  "--canvas-grid-size",
] as const;

const orderedLayers = [
  "ambient",
  "canvas",
  "canvas-controls",
  "presence",
  "object-toolbar",
  "chrome",
  "panels",
  "popover",
  "notification",
  "palette",
  "dialog",
  "terminal",
] as const;

describe("premium foundation contract", () => {
  it("defines every required semantic token with a safe fallback at use sites", () => {
    for (const token of requiredTokens) expect(styles).toContain(`${token}:`);
    expect(styles).toContain("var(--color-background-application, #eef0f5)");
    expect(styles).toContain("var(--focus-ring-color, #5146d8)");
  });

  it("keeps ADR layers unique and strictly ordered", () => {
    const values = orderedLayers.map((name) => {
      const match = styles.match(new RegExp(`--layer-${name}:\\s*(\\d+);`));
      expect(match, `missing --layer-${name}`).not.toBeNull();
      return Number(match?.[1]);
    });

    expect(new Set(values).size).toBe(values.length);
    expect(values).toEqual([...values].sort((left, right) => left - right));
    expect(styles).not.toMatch(/z-index:\s*-?\d/);
  });

  it("keeps empty portal hosts transparent to canvas pointer input", () => {
    expect(styles).toMatch(/\.portal-hosts,\s*\.portal-root\s*{[^}]*pointer-events:\s*none;/s);
    expect(styles).toMatch(/\.portal-root\s*>\s*\*\s*{[^}]*pointer-events:\s*auto;/s);
  });

  it("creates exactly one isolated application stacking context", () => {
    expect(styles).toMatch(/#application-root\s*{[^}]*isolation:\s*isolate;/s);
    expect(styles.match(/isolation:\s*isolate/g)).toHaveLength(1);
  });

  it("defines visible focus, reduced-motion, and forced-color behavior", () => {
    expect(styles).toContain(":focus-visible");
    expect(styles).toContain("@media (prefers-reduced-motion: reduce)");
    expect(styles).toContain("animation-iteration-count: 1 !important");
    expect(styles).toContain("@media (forced-colors: active)");
    expect(styles).toContain("forced-color-adjust");
  });

  it("defines dark-compatible semantic overrides without activating a switcher", () => {
    expect(styles).toContain('[data-theme="dark"]');
    expect(styles).not.toContain("prefers-color-scheme: dark");
  });

  it("keeps light-theme text, state, and collaborator colors at AA text contrast", () => {
    const token = (name: string): string => {
      const value = styles.match(new RegExp(`${name}:\\s*(#[0-9a-f]{6});`, "i"))?.[1];
      expect(value, `missing solid color ${name}`).toBeDefined();
      return value ?? "#000000";
    };
    const luminance = (hex: string): number => {
      const channels = [1, 3, 5].map(
        (start) => Number.parseInt(hex.slice(start, start + 2), 16) / 255,
      );
      const linear = channels.map((channel) =>
        channel <= 0.04045 ? channel / 12.92 : ((channel + 0.055) / 1.055) ** 2.4,
      );
      return 0.2126 * linear[0]! + 0.7152 * linear[1]! + 0.0722 * linear[2]!;
    };
    const contrast = (foreground: string, background: string): number => {
      const values = [luminance(foreground), luminance(background)].sort(
        (left, right) => right - left,
      );
      return (values[0]! + 0.05) / (values[1]! + 0.05);
    };

    const foregroundTokens = [
      "--color-text-primary",
      "--color-text-secondary",
      "--color-text-muted",
      "--color-action-base",
      "--color-success",
      "--color-warning",
      "--color-danger",
      "--color-information",
      "--color-reconnecting",
      "--color-recovering",
      "--color-unavailable",
      "--color-revoked",
      ...Array.from({ length: 8 }, (_, index) => `--color-collaborator-${index + 1}`),
    ];

    for (const name of foregroundTokens) {
      expect(contrast(token(name), "#ffffff"), name).toBeGreaterThanOrEqual(4.5);
    }
  });
});
