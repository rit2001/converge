import { access } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { expect, test } from "@playwright/test";
import {
  countOwnedPerformanceDatabases,
  createHundredObjectFixture,
  startProductionPerformanceTopology,
} from "./production-performance-topology";

const buildDirectory = fileURLToPath(new URL("../../apps/web/.next", import.meta.url));

test("forced partial startup cleans its database before the production smoke", async ({
  browser,
}) => {
  await expect(startProductionPerformanceTopology({ failAfter: "database" })).rejects.toThrow(
    "PERF_FORCED_PARTIAL_START",
  );
  const topology = await startProductionPerformanceTopology();
  try {
    expect(await countOwnedPerformanceDatabases()).toBe(1);
    expect(createHundredObjectFixture()).toHaveLength(100);
    const { page } = await topology.open(browser);
    const pageErrors: string[] = [];
    const hydrationErrors: string[] = [];
    page.on("pageerror", (error) => pageErrors.push(error.name));
    page.on("console", (message) => {
      if (/hydration/i.test(message.text())) hydrationErrors.push(message.type());
    });
    await expect(
      page.getByRole("button", { name: "Synchronization status: Synced" }),
    ).toBeVisible();
    await page.getByRole("button", { name: "Open layers panel" }).click();
    const objects = page
      .getByRole("list", { name: "Board objects, top to bottom" })
      .getByRole("listitem");
    await expect(objects).toHaveCount(100);
    expect(
      await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth),
    ).toBe(true);
    expect(pageErrors).toEqual([]);
    expect(hydrationErrors).toEqual([]);
  } finally {
    await topology.stop();
  }
  await expect(access(buildDirectory)).rejects.toThrow();
});
