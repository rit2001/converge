import { readFileSync } from "node:fs";
import { expect, it } from "vitest";

const source = readFileSync(new URL("../app/styles.css", import.meta.url), "utf8");

function tokenBlock(selector: string): string {
  const match = source.match(new RegExp(`${selector}\\s*\\{([\\s\\S]*?)\\n\\}`));
  if (!match?.[1]) throw new Error(`Missing token block: ${selector}`);
  return match[1];
}

function token(block: string, name: string): string {
  const match = block.match(new RegExp(`${name}:\\s*(#[0-9a-fA-F]{6})`));
  if (!match?.[1]) throw new Error(`Missing hexadecimal token: ${name}`);
  return match[1];
}

function luminance(hex: string): number {
  const channels = [1, 3, 5].map(
    (offset) => Number.parseInt(hex.slice(offset, offset + 2), 16) / 255,
  );
  return channels
    .map((channel) => (channel <= 0.04045 ? channel / 12.92 : ((channel + 0.055) / 1.055) ** 2.4))
    .reduce((total, channel, index) => total + channel * ([0.2126, 0.7152, 0.0722][index] ?? 0), 0);
}

function ratio(foreground: string, background: string): number {
  const [light, dark] = [luminance(foreground), luminance(background)].sort(
    (left, right) => right - left,
  );
  return ((light ?? 0) + 0.05) / ((dark ?? 0) + 0.05);
}

it("keeps critical semantic token pairs at AA contrast in light and dark themes", () => {
  const blocks = [tokenBlock(":root"), tokenBlock('\\[data-theme=\\"dark\\"\\]')];
  for (const block of blocks) {
    const pair = (foreground: string, background: string, minimum = 4.5): void => {
      expect(ratio(token(block, foreground), token(block, background))).toBeGreaterThanOrEqual(
        minimum,
      );
    };
    pair("--color-text-primary", "--color-background-application");
    pair("--color-text-primary", "--color-surface-primary");
    pair("--color-text-muted", "--color-surface-primary");
    pair("--color-action-on-base", "--color-action-base");
    pair("--color-success", "--color-success-subtle");
    pair("--color-warning", "--color-warning-subtle");
    pair("--color-danger", "--color-danger-subtle");
    pair("--color-information", "--color-information-subtle");
    pair("--color-text-inverse", "--color-surface-inverse");
    pair("--color-border-focus", "--color-surface-primary", 3);
    pair("--color-collaborator-1", "--color-background-canvas", 3);
    pair("--color-text-secondary", "--color-surface-primary");
  }
});
