import { expect, test } from "@playwright/test";

test("phone capability is view-only while panels and navigation remain available", async ({
  page,
}) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto("/studio");
  await expect(page.getByRole("button", { name: "Synchronization status: Synced" })).toBeVisible();
  await expect(page.getByLabel("View-only screen notice")).toContainText(
    "View-only on this screen",
  );
  await expect(page.getByTestId("add-rectangle")).toBeHidden();
  await page.keyboard.press("ControlOrMeta+K");
  const input = page.getByRole("combobox", { name: "Search commands" });
  await input.fill("Create sticky note at viewport center");
  await expect(
    page.getByRole("option", { name: /Create sticky note at viewport center/ }),
  ).toHaveAttribute("aria-disabled", "true");
  await page.keyboard.press("Escape");
  await page.getByRole("button", { name: "Open layers panel" }).click();
  await expect(page.getByRole("complementary", { name: "Board objects" })).toBeVisible();
  await expect
    .poll(() => page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth))
    .toBe(true);
});
