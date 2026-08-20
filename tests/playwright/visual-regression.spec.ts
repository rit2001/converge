import { expect, test, type Page } from "@playwright/test";

async function installDeterministicVisualPolicy(page: Page): Promise<void> {
  await page.addInitScript(() => {
    document.addEventListener("DOMContentLoaded", () => {
      const style = document.createElement("style");
      style.dataset.visualAcceptance = "true";
      style.textContent = `
        *, *::before, *::after {
          animation-delay: 0s !important;
          animation-duration: 0s !important;
          caret-color: transparent !important;
          scroll-behavior: auto !important;
          transition-delay: 0s !important;
          transition-duration: 0s !important;
        }
      `;
      document.head.append(style);
    });
  });
}

async function settle(page: Page): Promise<void> {
  await page.evaluate(async () => {
    await document.fonts.ready;
    await new Promise<void>((resolve) =>
      requestAnimationFrame(() => requestAnimationFrame(() => resolve())),
    );
  });
}

async function capture(page: Page, name: string, mask = [] as ReturnType<Page["locator"]>[]) {
  await settle(page);
  await expect(page).toHaveScreenshot(name, {
    animations: "disabled",
    caret: "hide",
    mask,
    maxDiffPixelRatio: 0.005,
    threshold: 0.15,
  });
}

test("accepted M3 surfaces remain visually stable across themes and capabilities", async ({
  page,
}) => {
  const pageErrors: string[] = [];
  const hydrationErrors: string[] = [];
  page.on("pageerror", (error) => pageErrors.push(error.name));
  page.on("console", (message) => {
    if (/hydration/i.test(message.text())) hydrationErrors.push(message.type());
  });
  await installDeterministicVisualPolicy(page);
  await page.setViewportSize({ width: 1440, height: 900 });
  await page.addInitScript(() => {
    if (!localStorage.getItem("converge:theme:v1"))
      localStorage.setItem(
        "converge:theme:v1",
        JSON.stringify({ version: 1, preference: "light" }),
      );
    localStorage.removeItem("converge:studio-onboarding:v1");
  });

  await page.goto("/");
  await expect(page.locator("html")).toHaveAttribute("data-theme", "light");
  await capture(page, "landing-light-desktop.png");
  await page.evaluate(() =>
    localStorage.setItem("converge:theme:v1", JSON.stringify({ version: 1, preference: "dark" })),
  );
  await page.reload();
  await expect(page.locator("html")).toHaveAttribute("data-theme", "dark");
  await capture(page, "landing-dark-desktop.png");

  await page.evaluate(() =>
    localStorage.setItem("converge:theme:v1", JSON.stringify({ version: 1, preference: "light" })),
  );
  await page.goto("/studio");
  await expect(page.getByRole("button", { name: "Synchronization status: Synced" })).toBeVisible();
  await expect(
    page.getByRole("complementary", { name: "First-run studio guidance" }),
  ).toBeVisible();
  await capture(page, "studio-onboarding-light-desktop.png");

  await page.getByRole("button", { name: "Not now" }).click();
  await page.getByTestId("add-rectangle").click();
  await page.getByTestId("add-sticky").click();
  await page.getByRole("button", { name: "Open layers panel" }).click();
  const list = page.getByRole("list", { name: "Board objects, top to bottom" });
  await expect(list.getByRole("listitem")).toHaveCount(2);
  await list.getByRole("button", { name: /Rectangle, bottom layer, 2 of 2/ }).click();
  await expect(page.getByLabel(/Shared rotation controls/)).toBeVisible();
  await capture(page, "studio-selection-layers-light-desktop.png");

  await page.getByRole("button", { name: "Close layers panel" }).click();
  await page.getByRole("button", { name: "Open studio help" }).click();
  const help = page.getByRole("dialog", { name: "Studio help" });
  await help.getByRole("radio", { name: "Dark" }).check();
  await expect(page.locator("html")).toHaveAttribute("data-theme", "dark");
  await page.keyboard.press("Escape");
  await page.getByRole("button", { name: "Open studio help" }).click();
  await capture(page, "studio-help-dark-desktop.png");
  await page.keyboard.press("Escape");

  await page.keyboard.press("ControlOrMeta+K");
  await expect(page.getByRole("dialog", { name: "Command palette" })).toBeVisible();
  await capture(page, "studio-palette-dark-desktop.png");
  await page.keyboard.press("Escape");

  await page.getByRole("button", { name: "Share board" }).click();
  const share = page.getByRole("dialog", { name: "Share this board" });
  await expect(share).toBeVisible();
  await settle(page);
  await expect(page).toHaveScreenshot("studio-share-dark-desktop.png", {
    animations: "disabled",
    caret: "hide",
    mask: [share.getByLabel("Board link")],
    maskColor: "#737182",
    maxDiffPixelRatio: 0.005,
    threshold: 0.15,
  });
  await page.keyboard.press("Escape");

  await page.setViewportSize({ width: 1024, height: 768 });
  await page.getByRole("button", { name: "Synchronization status: Synced" }).click();
  await expect(page.getByRole("region", { name: "Synchronization details" })).toBeVisible();
  await capture(page, "studio-synchronization-dark-compact.png");
  await page.keyboard.press("Escape");

  await page.setViewportSize({ width: 390, height: 844 });
  await expect(page.getByLabel("View-only screen notice")).toContainText(
    "View-only on this screen",
  );
  await page.getByRole("button", { name: "Open layers panel" }).click();
  await expect(page.getByRole("complementary", { name: "Board objects" })).toBeVisible();
  await capture(page, "studio-view-only-dark-phone.png");

  expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true);
  expect(pageErrors).toEqual([]);
  expect(hydrationErrors).toEqual([]);
});
