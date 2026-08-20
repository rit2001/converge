import { expect, test } from "@playwright/test";

test("command palette opens by keyboard, searches, executes one local command, and restores focus", async ({
  page,
}) => {
  await page.goto("/studio");
  await expect(page.getByRole("button", { name: "Synchronization status: Synced" })).toBeVisible();
  await page.keyboard.press("ControlOrMeta+K");
  const dialog = page.getByRole("dialog", { name: "Command palette" });
  await expect(dialog).toBeVisible();
  const search = dialog.getByRole("combobox", { name: "Search commands" });
  await expect(search).toBeFocused();
  await search.fill("hand");
  await page.keyboard.press("Enter");
  await expect(dialog).toBeHidden();
  await expect(page.getByRole("button", { name: "Pan tool" })).toHaveAttribute(
    "aria-pressed",
    "true",
  );
  const trigger = page.getByRole("button", { name: "Open command palette" });
  await trigger.focus();
  await page.keyboard.press("Enter");
  await page.keyboard.press("Escape");
  await expect(trigger).toBeFocused();
});
