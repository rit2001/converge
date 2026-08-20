import { expect, test } from "@playwright/test";

const key = "converge:studio-onboarding:v1";
test("first-run guidance persists bounded dismissal and Help is keyboard-safe", async ({
  page,
}) => {
  const errors: string[] = [];
  page.on("pageerror", (error) => errors.push(error.message));
  await page.goto("/");
  await page.evaluate((storageKey) => localStorage.removeItem(storageKey), key);
  await page.goto("/studio");
  await expect(page.getByRole("button", { name: "Synchronization status: Synced" })).toBeVisible();
  const welcome = page.getByRole("complementary", { name: "First-run studio guidance" });
  await expect(welcome.getByRole("heading", { name: "Your shared canvas is ready" })).toBeVisible();
  await welcome.getByRole("button", { name: "Add a sticky note" }).click();
  await expect(welcome).toBeHidden();
  expect(await page.evaluate((storageKey) => localStorage.getItem(storageKey), key)).toBe(
    '{"version":1,"state":"dismissed"}',
  );
  await page.reload();
  await expect(page.getByRole("button", { name: "Synchronization status: Synced" })).toBeVisible();
  await expect(welcome).toBeHidden();
  const trigger = page.getByRole("button", { name: "Open studio help" });
  await trigger.click();
  const help = page.getByRole("dialog", { name: "Studio help" });
  await expect(help).toBeVisible();
  await expect(help).toContainText("collaborator cursors are ephemeral");
  await expect(help).not.toContainText(/undo|invite|history|grouping|rename|comments|reorder/i);
  await page.keyboard.press("Escape");
  await expect(trigger).toBeFocused();
  await page.keyboard.press("?");
  await expect(help).toBeVisible();
  await page.keyboard.press("Escape");
  await page.setViewportSize({ width: 390, height: 844 });
  await expect
    .poll(() => page.evaluate(() => document.documentElement.scrollWidth <= innerWidth))
    .toBe(true);
  await expect(page.getByLabel("View-only screen notice")).toContainText(
    "View-only on this screen",
  );
  expect(errors).toEqual([]);
});
