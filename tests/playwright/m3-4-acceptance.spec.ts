import { expect, test, type Page } from "@playwright/test";
import { boardSnapshotSchema, type BoardSnapshot, type CanvasObject } from "@converge/protocol";

const API_URL = "http://127.0.0.1:4000";

async function snapshotFor(page: Page, boardId: string): Promise<BoardSnapshot> {
  const response = await page.request.get(`${API_URL}/v1/boards/${boardId}`);
  expect(response.ok()).toBe(true);
  return boardSnapshotSchema.parse(await response.json());
}

async function waitReady(page: Page): Promise<void> {
  await expect(page.getByRole("button", { name: "Synchronization status: Synced" })).toBeVisible();
}

async function openLayers(page: Page): Promise<void> {
  const trigger = page.getByRole("button", { name: "Open layers panel" });
  if ((await trigger.getAttribute("aria-expanded")) !== "true") await trigger.click();
  await expect(page.getByRole("heading", { name: "Layers" })).toBeVisible();
}

function objectOfKind(snapshot: BoardSnapshot, kind: CanvasObject["kind"]): CanvasObject {
  const object = snapshot.objects.find((candidate) => candidate.kind === kind);
  expect(object).toBeDefined();
  return object!;
}

async function dragWithAlt(
  page: Page,
  from: { x: number; y: number },
  to: { x: number; y: number },
) {
  const session = await page.context().newCDPSession(page);
  try {
    await session.send("Input.dispatchMouseEvent", {
      type: "mouseMoved",
      x: from.x,
      y: from.y,
      button: "none",
      buttons: 0,
      modifiers: 1,
    });
    await session.send("Input.dispatchMouseEvent", {
      type: "mousePressed",
      x: from.x,
      y: from.y,
      button: "left",
      buttons: 1,
      clickCount: 1,
      modifiers: 1,
    });
    await session.send("Input.dispatchMouseEvent", {
      type: "mouseMoved",
      x: to.x,
      y: to.y,
      button: "none",
      buttons: 1,
      modifiers: 1,
    });
    await session.send("Input.dispatchMouseEvent", {
      type: "mouseReleased",
      x: to.x,
      y: to.y,
      button: "left",
      buttons: 0,
      modifiers: 1,
    });
  } finally {
    await session.detach();
  }
}

test("M3.4 canvas interactions accept two-client local controls and authoritative transforms", async ({
  browser,
}) => {
  const contextA = await browser.newContext({ viewport: { width: 1440, height: 900 } });
  const contextB = await browser.newContext({ viewport: { width: 1024, height: 768 } });
  try {
    const pageA = await contextA.newPage();
    await pageA.goto("/studio");
    await waitReady(pageA);
    await expect.poll(() => new URL(pageA.url()).searchParams.get("board")).not.toBeNull();
    const boardId = new URL(pageA.url()).searchParams.get("board")!;

    const pageB = await contextB.newPage();
    await pageB.goto(pageA.url());
    await waitReady(pageB);

    await pageA.getByTestId("add-rectangle").click();
    await pageA.getByTestId("add-sticky").click();
    await expect.poll(async () => (await snapshotFor(pageA, boardId)).lastSeq).toBe(2);

    await openLayers(pageA);
    const layers = pageA.getByRole("list", { name: "Board layers, top to bottom" });
    await expect(
      layers.getByRole("button", { name: /Sticky note, top layer, 1 of 2/ }),
    ).toBeVisible();
    const rectangleLayer = layers.getByRole("button", { name: /Rectangle, bottom layer, 2 of 2/ });
    await rectangleLayer.focus();
    await pageA.keyboard.press("ArrowUp");
    await expect(
      layers.getByRole("button", { name: /Sticky note, top layer, 1 of 2/ }),
    ).toBeFocused();
    await pageA.keyboard.press("ArrowDown");
    await pageA.keyboard.press("Enter");
    await expect(rectangleLayer).toHaveAttribute("aria-pressed", "true");
    await pageA.keyboard.press("Escape");
    await expect(pageA.getByRole("button", { name: "Open layers panel" })).toBeFocused();
    await expect(pageA.locator("#workspace-diagnostics-panel")).toBeHidden();
    await expect(pageA.getByTestId("board-id")).toBeHidden();

    // Dragging the rectangle to x=141 is pulled three world units to the sticky's x=144 edge.
    await pageA.mouse.move(125, 105);
    await pageA.mouse.down();
    await pageA.mouse.move(146, 105, { steps: 3 });
    await pageA.mouse.up();
    await expect
      .poll(async () => objectOfKind(await snapshotFor(pageA, boardId), "rectangle").x)
      .toBe(144);

    // Alt bypass leaves the same object four world units off the reference edge.
    await dragWithAlt(pageA, { x: 149, y: 105 }, { x: 153, y: 105 });
    await expect
      .poll(async () => objectOfKind(await snapshotFor(pageA, boardId), "rectangle").x)
      .toBe(148);

    await openLayers(pageA);
    await pageA.getByRole("button", { name: "Hide Rectangle" }).click();
    await expect(pageA.getByRole("button", { name: "Show Rectangle" })).toBeVisible();
    await pageB.mouse.click(153, 105);
    await expect(
      pageB.getByRole("button", { name: "Rotate Rectangle 15° clockwise" }),
    ).toBeVisible();

    await pageA.getByRole("button", { name: "Show Rectangle" }).click();
    await rectangleLayer.click();
    await pageA.getByRole("button", { name: "Lock Rectangle" }).click();
    await expect(pageA.getByRole("button", { name: "Delete selected" })).toBeDisabled();
    await expect(pageA.getByRole("button", { name: "Rotate Rectangle 15° clockwise" })).toHaveCount(
      0,
    );
    const beforeLockedAttempt = (await snapshotFor(pageA, boardId)).lastSeq;
    await pageA.mouse.move(150, 105);
    await pageA.mouse.down();
    await pageA.mouse.move(190, 105, { steps: 2 });
    await pageA.mouse.up();
    expect((await snapshotFor(pageA, boardId)).lastSeq).toBe(beforeLockedAttempt);

    await pageB.getByRole("button", { name: "Rotate Rectangle 15° clockwise" }).click();
    await pageB.getByRole("button", { name: "Rotate Rectangle 15° counterclockwise" }).click();
    await pageB.getByRole("button", { name: "Reset Rectangle rotation to 0°" }).click();
    await expect
      .poll(async () => objectOfKind(await snapshotFor(pageB, boardId), "rectangle").rotation)
      .toBe(0);

    await pageA.getByRole("button", { name: "Unlock Rectangle" }).click();
    await rectangleLayer.click();
    await expect(
      pageA.getByRole("button", { name: "Rotate Rectangle 15° clockwise" }),
    ).toBeVisible();
    await pageA.getByRole("button", { name: "Rotate Rectangle 15° clockwise" }).click();
    await expect
      .poll(async () => objectOfKind(await snapshotFor(pageB, boardId), "rectangle").rotation)
      .toBe(15);

    await pageA.reload();
    await waitReady(pageA);
    await openLayers(pageA);
    await expect(pageA.getByRole("button", { name: "Hide Rectangle" })).toBeVisible();
    await expect(pageA.getByRole("button", { name: "Lock Rectangle" })).toBeVisible();

    await pageA.setViewportSize({ width: 390, height: 844 });
    await expect(pageA.getByLabel("Desktop editor notice")).toBeVisible();
    await expect
      .poll(() => pageA.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth))
      .toBe(true);
    await pageB.setViewportSize({ width: 1024, height: 768 });
    await expect
      .poll(() => pageB.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth))
      .toBe(true);
    await pageA.setViewportSize({ width: 1440, height: 900 });
    await pageA.emulateMedia({ forcedColors: "active", reducedMotion: "reduce" });
    await expect(pageA.getByRole("button", { name: "Open layers panel" })).toBeVisible();
  } finally {
    await contextB.close();
    await contextA.close();
  }
});
