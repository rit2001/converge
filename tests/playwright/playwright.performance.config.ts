import { defineConfig } from "@playwright/test";

export default defineConfig({
  testDir: ".",
  testMatch: "production-performance-topology.spec.ts",
  timeout: 120_000,
  expect: { timeout: 20_000 },
  workers: 1,
  fullyParallel: false,
  use: { viewport: { width: 1440, height: 900 }, trace: "retain-on-failure" },
});
