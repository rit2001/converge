import AxeBuilder from "@axe-core/playwright";
import { expect, test, type Page } from "@playwright/test";

async function audit(page: Page, surface: string): Promise<void> {
  const result = await new AxeBuilder({ page })
    .withTags(["wcag2a", "wcag2aa", "wcag22a", "wcag22aa"])
    .analyze();
  const blocking = result.violations.filter(
    (violation) => violation.impact === "critical" || violation.impact === "serious",
  );
  expect(
    blocking,
    `${surface}: ${blocking
      .map(
        (item) =>
          `${item.id} (${item.impact}): ${item.help}; ${item.nodes
            .slice(0, 3)
            .map((node) => node.target.join(" "))
            .join(", ")}`,
      )
      .join(" | ")}`,
  ).toEqual([]);
}

test("WCAG A/AA gate covers public M3 surfaces", async ({ page }) => {
  await page.goto("/");
  await audit(page, "landing");
  await page.goto("/studio");
  await expect(page.getByRole("button", { name: "Synchronization status: Synced" })).toBeVisible();
  await audit(page, "empty studio");
  await page.getByTestId("add-rectangle").click();
  await page.getByRole("button", { name: "Open layers panel" }).click();
  await audit(page, "object navigator");
  await page.keyboard.press("ControlOrMeta+K");
  await audit(page, "command palette");
  await page.keyboard.press("Escape");
  await page.getByLabel("Open studio help").click();
  await audit(page, "help dialog");
  await page.keyboard.press("Escape");
  await page.getByLabel("Share board").click();
  await audit(page, "share dialog");
});
