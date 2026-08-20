import { expect, test, type Page } from "@playwright/test";
import { emptyBoardState, hashBoardState } from "@converge/canvas-engine";
import { boardSnapshotSchema, type BoardSnapshot, type CanvasObject } from "@converge/protocol";

const API_URL = "http://127.0.0.1:4000";

async function snapshotFor(page: Page, boardId: string) {
  const response = await page.request.get(`${API_URL}/v1/boards/${boardId}`);
  expect(response.ok()).toBe(true);
  return boardSnapshotSchema.parse(await response.json());
}

async function snapshotHash(snapshot: BoardSnapshot): Promise<string> {
  const state = emptyBoardState();
  state.lastSeq = snapshot.lastSeq;
  for (const object of snapshot.objects) {
    state.objects[object.id] = {
      value: object,
      createdSeq: 0,
      updatedSeq: snapshot.lastSeq,
      deletedSeq: null,
      fieldSeq: {},
    };
    state.order.push(object.id);
  }
  return hashBoardState(state);
}

async function expectClientAtSnapshot(
  page: Page,
  snapshot: BoardSnapshot,
  expectedHash: string,
): Promise<void> {
  await expect(page.getByRole("button", { name: "Synchronization status: Synced" })).toBeVisible();
  await expect(page.getByTestId("board-id")).toHaveText(snapshot.id);
  await expect(page.getByTestId("last-seq")).toHaveText(String(snapshot.lastSeq));
  await expect(page.getByTestId("hash-board-id")).toHaveText(snapshot.id);
  await expect(page.getByTestId("hash-seq")).toHaveText(String(snapshot.lastSeq));
  await expect(page.getByTestId("hash-session-generation")).toHaveText(/^\d+$/);
  await expect(page.getByTestId("hash-status")).toHaveText("ready");
  await expect(page.getByTestId("state-hash")).toHaveText(expectedHash);
  const committed = JSON.parse(
    (await page.getByTestId("committed-objects").textContent()) ?? "[]",
  ) as CanvasObject[];
  expect(committed).toEqual(snapshot.objects);
}

async function useUuidSequence(page: Page, values: string[]): Promise<void> {
  await page.evaluate((sequence) => {
    const fallback = crypto.randomUUID.bind(crypto);
    let index = 0;
    Object.defineProperty(crypto, "randomUUID", {
      configurable: true,
      value: () => sequence[index++] ?? fallback(),
    });
  }, values);
}

async function canvasPixel(page: Page, x: number, y: number): Promise<number[]> {
  return page
    .locator(".canvas-shell canvas")
    .first()
    .evaluate(
      (node, point) => {
        const canvas = node as HTMLCanvasElement;
        const context = canvas.getContext("2d");
        if (!context) throw new Error("Canvas context is unavailable");
        const bounds = canvas.getBoundingClientRect();
        const pixel = context.getImageData(
          Math.floor(point.x * (canvas.width / bounds.width)),
          Math.floor(point.y * (canvas.height / bounds.height)),
          1,
          1,
        ).data;
        return [...pixel];
      },
      { x, y },
    );
}

function expectStickyPixel(pixel: number[]): void {
  expect(pixel[0]).toBeGreaterThan(245);
  expect(pixel[1]).toBeGreaterThan(220);
  expect(pixel[2]).toBeGreaterThan(125);
  expect(pixel[2]).toBeLessThan(155);
  expect(pixel[3]).toBe(255);
}

test("two independent clients converge after editing the same board", async ({ browser }) => {
  const contextA = await browser.newContext();
  const pageA = await contextA.newPage();
  await pageA.goto("/studio");
  await expect(pageA.getByRole("button", { name: "Synchronization status: Synced" })).toBeVisible();
  await expect.poll(() => new URL(pageA.url()).searchParams.get("board")).not.toBeNull();
  const sharedUrl = pageA.url();
  const boardId = new URL(sharedUrl).searchParams.get("board");
  expect(boardId).not.toBeNull();

  const contextB = await browser.newContext();
  const pageB = await contextB.newPage();
  await pageB.goto(sharedUrl);
  await expect(pageB.getByRole("button", { name: "Synchronization status: Synced" })).toBeVisible();

  await Promise.all([
    pageA.getByTestId("add-rectangle").click(),
    pageB.getByTestId("add-sticky").click(),
  ]);
  await expect(pageA.getByTestId("pending-count")).toHaveText("0");
  await expect(pageB.getByTestId("pending-count")).toHaveText("0");
  await expect(pageA.getByTestId("last-seq")).toHaveText("2");
  await expect(pageB.getByTestId("last-seq")).toHaveText("2");
  const snapshot = await snapshotFor(pageA, boardId ?? "");
  expect(snapshot.lastSeq).toBe(2);
  expect(snapshot.objects.map((object) => object.kind).sort()).toEqual(["rectangle", "sticky"]);
  const expectedHash = await snapshotHash(snapshot);
  await expectClientAtSnapshot(pageA, snapshot, expectedHash);
  await expectClientAtSnapshot(pageB, snapshot, expectedHash);

  await contextA.close();
  await contextB.close();
});

test("a disconnected client catches up on reconnect without a trigger mutation", async ({
  browser,
}) => {
  const contextA = await browser.newContext();
  const pageA = await contextA.newPage();
  await pageA.goto("/studio");
  await expect(pageA.getByRole("button", { name: "Synchronization status: Synced" })).toBeVisible();
  await expect.poll(() => new URL(pageA.url()).searchParams.get("board")).not.toBeNull();
  const boardId = new URL(pageA.url()).searchParams.get("board");
  expect(boardId).not.toBeNull();

  const contextB = await browser.newContext();
  const pageB = await contextB.newPage();
  await pageB.goto(pageA.url());
  await expect(pageB.getByRole("button", { name: "Synchronization status: Synced" })).toBeVisible();

  await contextA.setOffline(true);
  await expect(
    pageA.getByRole("button", { name: "Synchronization status: Reconnecting…" }),
  ).toBeVisible();
  await pageB.getByTestId("add-rectangle").click();
  await pageB.getByTestId("add-sticky").click();
  await expect(pageB.getByTestId("pending-count")).toHaveText("0");
  await expect(pageB.getByTestId("last-seq")).toHaveText("2");

  await contextA.setOffline(false);
  const snapshot = await snapshotFor(pageB, boardId ?? "");
  expect(snapshot.lastSeq).toBe(2);
  expect(snapshot.objects.map((object) => object.kind).sort()).toEqual(["rectangle", "sticky"]);
  const expectedHash = await snapshotHash(snapshot);
  await expectClientAtSnapshot(pageB, snapshot, expectedHash);
  await expectClientAtSnapshot(pageA, snapshot, expectedHash);

  await contextA.close();
  await contextB.close();
});

test("overlapping objects retain their topmost creation order after reload and reconnect", async ({
  browser,
}) => {
  const highId = "f0000000-0000-4000-8000-000000000001";
  const lowId = "10000000-0000-4000-8000-000000000002";
  const contextA = await browser.newContext();
  const pageA = await contextA.newPage();
  await pageA.goto("/studio");
  await expect(pageA.getByRole("button", { name: "Synchronization status: Synced" })).toBeVisible();
  await expect.poll(() => new URL(pageA.url()).searchParams.get("board")).not.toBeNull();
  const boardId = new URL(pageA.url()).searchParams.get("board");
  expect(boardId).not.toBeNull();
  await useUuidSequence(pageA, [
    highId,
    "40000000-0000-4000-8000-000000000011",
    lowId,
    "40000000-0000-4000-8000-000000000012",
  ]);

  await pageA.getByTestId("add-rectangle").click();
  await expect(pageA.getByTestId("pending-count")).toHaveText("0");
  await pageA.getByTestId("add-sticky").click();
  await expect(pageA.getByTestId("pending-count")).toHaveText("0");
  await expect(pageA.getByTestId("last-seq")).toHaveText("2");
  const snapshot = await snapshotFor(pageA, boardId ?? "");
  expect(snapshot.objects.map(({ id }) => id)).toEqual([highId, lowId]);
  const expectedHash = await snapshotHash(snapshot);
  await expectClientAtSnapshot(pageA, snapshot, expectedHash);
  expectStickyPixel(await canvasPixel(pageA, 285, 195));

  const contextB = await browser.newContext();
  const pageB = await contextB.newPage();
  await pageB.goto(pageA.url());
  await expectClientAtSnapshot(pageB, snapshot, expectedHash);
  expectStickyPixel(await canvasPixel(pageB, 285, 195));

  await pageB.reload();
  await expectClientAtSnapshot(pageB, snapshot, expectedHash);
  expectStickyPixel(await canvasPixel(pageB, 285, 195));

  await contextB.setOffline(true);
  await expect(
    pageB.getByRole("button", { name: "Synchronization status: Reconnecting…" }),
  ).toBeVisible();
  await contextB.setOffline(false);
  await expectClientAtSnapshot(pageB, snapshot, expectedHash);
  expectStickyPixel(await canvasPixel(pageB, 285, 195));

  await contextA.close();
  await contextB.close();
});
