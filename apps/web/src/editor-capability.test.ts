import { expect, it } from "vitest";
import { deriveEditorCapability } from "./editor-capability";

it("derives full, compact, and phone view-only capability without user-agent evidence", () => {
  expect(deriveEditorCapability({ width: 1440, height: 900, coarsePointer: false })).toBe(
    "full_edit",
  );
  expect(deriveEditorCapability({ width: 820, height: 1180, coarsePointer: true })).toBe(
    "compact_edit",
  );
  expect(deriveEditorCapability({ width: 390, height: 844, coarsePointer: true })).toBe(
    "view_only",
  );
  expect(deriveEditorCapability({ width: 844, height: 390, coarsePointer: true })).toBe(
    "view_only",
  );
  expect(deriveEditorCapability({ width: 900, height: 500, coarsePointer: false })).toBe(
    "compact_edit",
  );
});
