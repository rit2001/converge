import { defineConfig } from "@playwright/test";

/** M3.5C test-owned topology; it intentionally has no legacy single-replica webServer. */
export default defineConfig({
  testDir: ".",
  testMatch: "m3-5c*.spec.ts",
  timeout: 90_000,
  expect: { timeout: 15_000 },
  // Each topology owns the same local Redis/PostgreSQL service pair and client-count boundary.
  workers: 1,
  fullyParallel: false,
  use: { trace: "retain-on-failure" },
});
