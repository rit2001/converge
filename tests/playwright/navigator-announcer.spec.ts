import { expect, test, type Page } from "@playwright/test";

async function waitReady(page: Page): Promise<void> {
  await expect(page.getByRole("button", { name: "Synchronization status: Synced" })).toBeVisible();
}

test("Layers is the keyboard canvas alternative and completed actions use one polite announcer", async ({
  page,
}) => {
  await page.goto("/studio");
  await waitReady(page);

  const announcer = page.locator("output[data-generation]");
  await expect(announcer).toHaveCount(1);
  await page.getByTestId("add-rectangle").click();
  await expect(announcer).toHaveText("Rectangle created.");
  await page.getByTestId("add-sticky").click();
  await expect(announcer).toHaveText("Sticky note created.");

  const trigger = page.getByRole("button", { name: "Open layers panel" });
  await trigger.click();
  const list = page.getByRole("list", { name: "Board objects, top to bottom" });
  await expect(list).toBeVisible();
  await expect(list.locator("li")).toHaveCount(2);
  await expect(list).not.toContainText(
    /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/i,
  );

  const sticky = list.getByRole("button", { name: /Sticky note, top layer, 1 of 2/ });
  const rectangle = list.getByRole("button", { name: /Rectangle, bottom layer, 2 of 2/ });
  await rectangle.focus();
  await page.keyboard.press("Home");
  await expect(sticky).toBeFocused();
  await page.keyboard.press("End");
  await expect(rectangle).toBeFocused();
  await page.keyboard.press("Enter");
  await expect(rectangle).toHaveAttribute("aria-pressed", "true");
  await expect(announcer).toHaveText("Rectangle selected.");

  await page.keyboard.press("Tab");
  await expect(page.getByRole("button", { name: "Hide Rectangle" })).toBeFocused();
  await page.keyboard.press("Enter");
  await expect(announcer).toHaveText("Rectangle hidden in this view.");
  await page.getByRole("button", { name: "Show Rectangle" }).click();
  await expect(announcer).toHaveText("Rectangle shown in this view.");
  await page.getByRole("button", { name: "Lock Rectangle" }).click();
  await expect(announcer).toHaveText("Rectangle locked in this view.");
  await page.getByRole("button", { name: "Unlock Rectangle" }).click();
  await expect(announcer).toHaveText("Rectangle unlocked in this view.");

  await rectangle.click();
  await expect(rectangle).toHaveAttribute("aria-pressed", "true");
  await rectangle.focus();
  await page.getByRole("button", { name: "Delete selected" }).click();
  await expect(announcer).toHaveText("Rectangle deleted.");
  await expect(list.getByRole("button", { name: /Sticky note, top layer, 1 of 1/ })).toBeFocused();
  await page.keyboard.press("Escape");
  await expect(trigger).toBeFocused();
});
