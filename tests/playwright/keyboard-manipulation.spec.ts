import { expect, test, type Page } from "@playwright/test";
import { boardSnapshotSchema, type BoardSnapshot } from "@converge/protocol";

const API_URL = "http://127.0.0.1:4000";

async function snapshot(page: Page, boardId: string): Promise<BoardSnapshot> {
  const response = await page.request.get(`${API_URL}/v1/boards/${boardId}`);
  expect(response.ok()).toBe(true);
  return boardSnapshotSchema.parse(await response.json());
}

async function palette(page: Page, query: string): Promise<void> {
  await page.keyboard.press("ControlOrMeta+K");
  const input = page.getByRole("combobox", { name: "Search commands" });
  await input.fill("");
  await input.pressSequentially(query);
  await page.keyboard.press("Enter");
}

test("keyboard-only creation and manipulation use one authoritative command per accepted key", async ({
  page,
}) => {
  await page.goto("/studio");
  await expect(page.getByRole("button", { name: "Synchronization status: Synced" })).toBeVisible();
  const boardId = new URL(page.url()).searchParams.get("board")!;
  const announcer = page.locator("output[data-generation]");

  await palette(page, "Create sticky note at viewport center");
  await expect(announcer).toHaveText("Sticky note created.");
  await expect.poll(async () => (await snapshot(page, boardId)).lastSeq).toBe(1);

  await palette(page, "Open Layers");
  const row = page
    .getByRole("list", { name: "Board objects, top to bottom" })
    .getByRole("button", { name: /Sticky note, top layer, 1 of 1/ });
  await row.focus();
  await page.keyboard.press("Enter");
  await palette(page, "Focus canvas");
  const canvas = page.getByLabel("Canvas editing surface");
  await expect(canvas).toBeFocused();

  await page.keyboard.press("ArrowRight");
  await expect(announcer).toHaveText(/Sticky note moved to x/);
  await page.keyboard.press("Shift+ArrowDown");
  await page.keyboard.press("Alt+ArrowRight");
  await expect(announcer).toHaveText(/Sticky note resized to width/);
  await page.keyboard.press("Alt+Shift+ArrowDown");
  await expect.poll(async () => (await snapshot(page, boardId)).lastSeq).toBe(5);

  await palette(page, "Lock selected object in this view");
  await palette(page, "Focus canvas");
  const beforeBlocked = (await snapshot(page, boardId)).lastSeq;
  await page.keyboard.press("ArrowRight");
  expect((await snapshot(page, boardId)).lastSeq).toBe(beforeBlocked);

  await palette(page, "Open Layers");
  await row.focus();
  await page.keyboard.press("Tab");
  await page.keyboard.press("Tab");
  await page.keyboard.press("Enter");
  await palette(page, "Focus canvas");
  await page.keyboard.press("Delete");
  await expect(announcer).toHaveText("Sticky note deleted.");
  await expect.poll(async () => (await snapshot(page, boardId)).objects).toHaveLength(0);
});
