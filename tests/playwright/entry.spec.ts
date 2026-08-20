import { expect, test } from "@playwright/test";

test("landing remains inert until keyboard entry and studio reload preserves the board", async ({
  page,
}) => {
  const operationalRequests: Array<{ method: string; url: string }> = [];
  page.on("request", (request) => {
    if (request.url().startsWith("http://127.0.0.1:4000")) {
      operationalRequests.push({ method: request.method(), url: request.url() });
    }
  });

  await page.goto("/");
  await page.waitForLoadState("networkidle");
  await expect(page.getByRole("heading", { level: 1 })).toHaveText(
    "Shared thinking that survives the network.",
  );
  expect(operationalRequests).toEqual([]);

  const primaryAction = page.getByRole("link", { name: "Open the studio" }).first();
  await primaryAction.focus();
  await expect(primaryAction).toBeFocused();
  await page.keyboard.press("Enter");

  await expect(page).toHaveURL(/\/studio\?board=/);
  await expect(page.locator("header .connection")).toHaveText("ready");
  const studioUrl = page.url();
  expect(
    operationalRequests.filter(
      ({ method, url }) => method === "POST" && new URL(url).pathname === "/v1/boards",
    ),
  ).toHaveLength(1);

  await page.reload();
  await expect(page).toHaveURL(studioUrl);
  await expect(page.locator("header .connection")).toHaveText("ready");
  expect(
    operationalRequests.filter(
      ({ method, url }) => method === "POST" && new URL(url).pathname === "/v1/boards",
    ),
  ).toHaveLength(1);
});
