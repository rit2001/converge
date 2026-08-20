import { defineConfig } from "@playwright/test";

const databaseUrl =
  process.env.DATABASE_URL ?? "postgresql://converge:converge@localhost:55432/converge";
export default defineConfig({
  testDir: ".",
  testMatch: "*.spec.ts",
  testIgnore: ["m3-5c*.spec.ts", "production-performance-topology.spec.ts"],
  timeout: 30_000,
  expect: { timeout: 10_000 },
  snapshotPathTemplate: "{testDir}/{testFilePath}-snapshots/{arg}{ext}",
  fullyParallel: false,
  use: { baseURL: "http://127.0.0.1:3000", trace: "retain-on-failure" },
  webServer: [
    {
      command: "pnpm --filter @converge/database migrate && pnpm --filter @converge/api start",
      port: 4000,
      reuseExistingServer: !process.env.CI,
      env: {
        DATABASE_URL: databaseUrl,
        WEB_ORIGIN: "http://127.0.0.1:3000",
        DEV_AUTH_USER_ID: "00000000-0000-4000-8000-000000000001",
        NODE_ENV: "test",
        LOG_LEVEL: "warn",
      },
    },
    {
      command: "pnpm --filter @converge/web dev",
      port: 3000,
      reuseExistingServer: !process.env.CI,
      env: { NEXT_PUBLIC_API_URL: "http://127.0.0.1:4000" },
    },
  ],
});
