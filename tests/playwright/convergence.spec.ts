import { expect, test } from "@playwright/test";

test("two independent clients converge after editing the same board", async ({ browser }) => {
  const contextA = await browser.newContext();
  const pageA = await contextA.newPage();
  await pageA.goto("/");
  await expect(pageA.locator("header .connection")).toHaveText("ready");
  await expect.poll(() => new URL(pageA.url()).searchParams.get("board")).not.toBeNull();
  const sharedUrl = pageA.url();

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
  await expect
    .poll(async () => {
      const [left, right] = await Promise.all([
        pageA.getByTestId("state-hash").textContent(),
        pageB.getByTestId("state-hash").textContent(),
      ]);
      return left === right && /^[a-f0-9]{64}$/.test(left ?? "");
    })
    .toBe(true);

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
  await expect(pageA.locator("header .connection")).toHaveText("ready");
  await expect(pageA.getByTestId("last-seq")).toHaveText("2");
  await expect
    .poll(async () => {
      const [left, right] = await Promise.all([
        pageA.getByTestId("state-hash").textContent(),
        pageB.getByTestId("state-hash").textContent(),
      ]);
      return left === right && /^[a-f0-9]{64}$/.test(left ?? "");
    })
    .toBe(true);

  await contextA.close();
  await contextB.close();
});
