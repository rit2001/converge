import { expect, test, type Page } from "@playwright/test";
import { emptyBoardState, hashBoardState } from "@converge/canvas-engine";
import { boardSnapshotSchema, type BoardSnapshot, type CanvasObject } from "@converge/protocol";

const API_URL = "http://127.0.0.1:4000";

function sortObjects(objects: CanvasObject[]): CanvasObject[] {
  return [...objects].sort((left, right) => left.id.localeCompare(right.id));
}

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
  await expect(page.locator("header .connection")).toHaveText("ready");
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
  expect(sortObjects(committed)).toEqual(sortObjects(snapshot.objects));
}

test("two independent clients converge after editing the same board", async ({ browser }) => {
  const contextA = await browser.newContext();
  const pageA = await contextA.newPage();
  await pageA.goto("/");
  await expect(pageA.locator("header .connection")).toHaveText("ready");
  await expect.poll(() => new URL(pageA.url()).searchParams.get("board")).not.toBeNull();
  const sharedUrl = pageA.url();
  const boardId = new URL(sharedUrl).searchParams.get("board");
  expect(boardId).not.toBeNull();

  const contextB = await browser.newContext();
  const pageB = await contextB.newPage();
  await pageB.goto(sharedUrl);
  await expect(pageB.locator("header .connection")).toHaveText("ready");

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
  await pageA.goto("/");
  await expect(pageA.locator("header .connection")).toHaveText("ready");
  await expect.poll(() => new URL(pageA.url()).searchParams.get("board")).not.toBeNull();
  const boardId = new URL(pageA.url()).searchParams.get("board");
  expect(boardId).not.toBeNull();

  const contextB = await browser.newContext();
  const pageB = await contextB.newPage();
  await pageB.goto(pageA.url());
  await expect(pageB.locator("header .connection")).toHaveText("ready");

  await contextA.setOffline(true);
  await expect(pageA.locator("header .connection")).toHaveText("disconnected");
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
