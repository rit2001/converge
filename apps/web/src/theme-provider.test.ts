import { expect, it } from "vitest";
import { effectiveTheme, parseThemePreference } from "./theme-provider";

it("strictly parses only the bounded versioned preference record", () => {
  expect(parseThemePreference('{"version":1,"preference":"dark"}')).toBe("dark");
  expect(parseThemePreference('{"version":2,"preference":"dark"}')).toBe("system");
  expect(parseThemePreference('{"version":1,"preference":"dark","extra":true}')).toBe("system");
  expect(parseThemePreference("not json")).toBe("system");
});

it("keeps preference and effective system resolution distinct", () => {
  expect(effectiveTheme("system", true)).toBe("dark");
  expect(effectiveTheme("system", false)).toBe("light");
  expect(effectiveTheme("light", true)).toBe("light");
  expect(effectiveTheme("dark", false)).toBe("dark");
});
