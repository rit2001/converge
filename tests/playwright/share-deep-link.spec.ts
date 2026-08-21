import { expect, test } from "@playwright/test";

test("authenticated deep link and Share dialog preserve the exact board path", async ({
  page,
  context,
}) => {
  const copied: string[] = [];
  await context.grantPermissions(["clipboard-read", "clipboard-write"]);
  await page.goto("/studio");
  await expect(page.getByRole("button", { name: "Synchronization status: Synced" })).toBeVisible();
  const boardId = new URL(page.url()).searchParams.get("board");
  expect(boardId).toMatch(/^[0-9a-f-]{36}$/i);
  await page.getByLabel("Share board").click();
  const dialog = page.getByRole("dialog", { name: "Share this board" });
  await expect(dialog).toBeVisible();
  const link = dialog.getByLabel("Board link");
  const expected = `${new URL(page.url()).origin}/studio/${boardId}`;
  await expect(link).toHaveValue(expected);
  await page.evaluate(() =>
    Object.defineProperty(navigator, "clipboard", {
      configurable: true,
      value: {
        writeText: (value: string) => {
          (window as Window & { __copied?: string[] }).__copied = [
            ...((window as Window & { __copied?: string[] }).__copied ?? []),
            value,
          ];
          return Promise.resolve();
        },
      },
    }),
  );
  await dialog.getByRole("button", { name: "Copy link" }).click();
  await expect(dialog).toContainText("Link copied.");
  copied.push(
    ...(await page.evaluate(() => (window as Window & { __copied?: string[] }).__copied ?? [])),
  );
  expect(copied).toEqual([expected]);
  await page.keyboard.press("Escape");
  await expect(page.getByLabel("Share board")).toBeFocused();
  await page.goto(expected);
  await expect(page.getByRole("button", { name: "Synchronization status: Synced" })).toBeVisible();
  expect(new URL(page.url()).pathname).toBe(`/studio/${boardId}`);
  await page.reload();
  await expect(page.getByRole("button", { name: "Synchronization status: Synced" })).toBeVisible();
  await page.goto("/studio/not-a-board");
  await expect(page.getByRole("heading", { name: "Board unavailable" })).toBeVisible();
});
