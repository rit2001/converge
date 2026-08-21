import { createHash } from "node:crypto";
import { expect, test, type Page } from "@playwright/test";
import { startDualReplicaBrowserTopology } from "./dual-replica-topology.js";

type SnapshotWire = {
  selfPresenceSessionId: string;
  participants: Array<{ presenceSessionId: string; userId: string }>;
};
type UpsertWire = {
  participant: {
    presenceSessionId: string;
    userId: string;
    revision: number;
    cursor: { x: number; y: number } | null;
  };
};

async function capturePresence(page: Page): Promise<{
  snapshots: SnapshotWire[];
  upserts: UpsertWire[];
}> {
  const evidence = { snapshots: [] as SnapshotWire[], upserts: [] as UpsertWire[] };
  const session = await page.context().newCDPSession(page);
  await session.send("Network.enable");
  session.on("Network.webSocketFrameReceived", ({ response }) => {
    if (!response.payloadData.startsWith("42")) return;
    try {
      const [event, value] = JSON.parse(response.payloadData.slice(2)) as [string, unknown];
      if (event === "board:presence-snapshot") evidence.snapshots.push(value as SnapshotWire);
      if (event === "presence:participant-upsert") evidence.upserts.push(value as UpsertWire);
    } catch {
      // This is only bounded wire observation; malformed frames are not product evidence.
    }
  });
  return evidence;
}

async function ready(page: Page): Promise<void> {
  await expect(page.getByRole("button", { name: "Synchronization status: Synced" })).toBeVisible();
}

async function roster(page: Page): Promise<ReturnType<Page["getByRole"]>> {
  const trigger = page.getByRole("button", { name: /^Collaborators:/ });
  if ((await trigger.getAttribute("aria-expanded")) !== "true") await trigger.click();
  return page.getByRole("region", { name: "Live collaborators" });
}

async function durableOperationCount(
  pool: { query<T>(text: string, values: readonly unknown[]): Promise<{ rows: T[] }> },
  boardId: string,
): Promise<number> {
  const result = await pool.query<{ count: string }>(
    "SELECT count(*)::text AS count FROM board_operations WHERE board_id = $1",
    [boardId],
  );
  return Number(result.rows[0]?.count ?? 0);
}

async function canvasDigest(page: Page): Promise<string> {
  const box = await page.locator(".canvas-shell").boundingBox();
  if (!box) throw new Error("Canvas shell unavailable");
  const image = await page.screenshot({ clip: box });
  return createHash("sha256").update(image).digest("hex");
}

test.describe.serial("M3.5C final browser acceptance", () => {
  test("keeps synchronization truthful while real cross-replica presence is grouped, rendered, fenced, and recovered", async ({
    browser,
  }) => {
    const topology = await startDualReplicaBrowserTopology();
    const pageErrors: Error[] = [];
    try {
      const a = await topology.openA(browser);
      const a2 = await topology.openA(browser);
      const b = await topology.openB(browser);
      await Promise.all([
        a.page.setViewportSize({ width: 1440, height: 900 }),
        a2.page.setViewportSize({ width: 1024, height: 768 }),
        b.page.setViewportSize({ width: 1440, height: 900 }),
      ]);
      for (const page of [a.page, a2.page, b.page])
        page.on("pageerror", (error) => pageErrors.push(error));
      const evidenceA = await capturePresence(a.page);
      const evidenceA2 = await capturePresence(a2.page);
      const evidenceB = await capturePresence(b.page);

      // A reload gives the CDP observers a complete self-specific admission sequence.
      await Promise.all([a.page.reload(), a2.page.reload(), b.page.reload()]);
      await Promise.all([ready(a.page), ready(a2.page), ready(b.page)]);
      await expect.poll(() => evidenceA.snapshots.length).toBeGreaterThan(0);
      await expect.poll(() => evidenceA2.snapshots.length).toBeGreaterThan(0);
      await expect.poll(() => evidenceB.snapshots.length).toBeGreaterThan(0);
      const aSelf = evidenceA.snapshots.at(-1)!;
      const a2Self = evidenceA2.snapshots.at(-1)!;
      const bSelf = evidenceB.snapshots.at(-1)!;
      expect(aSelf.selfPresenceSessionId).not.toBe(a2Self.selfPresenceSessionId);
      expect(
        aSelf.participants.find(
          (participant) => participant.presenceSessionId === aSelf.selfPresenceSessionId,
        )?.userId,
      ).toBe(topology.owner.id);
      expect(
        a2Self.participants.find(
          (participant) => participant.presenceSessionId === a2Self.selfPresenceSessionId,
        )?.userId,
      ).toBe(topology.owner.id);
      expect(
        bSelf.participants.find(
          (participant) => participant.presenceSessionId === bSelf.selfPresenceSessionId,
        )?.userId,
      ).toBe(topology.editor.id);

      const rosterB = await roster(b.page);
      await expect(rosterB.getByText("You", { exact: true })).toBeVisible();
      await expect(rosterB.getByText(topology.owner.displayName, { exact: true })).toBeVisible();
      await expect(rosterB.getByText("Active", { exact: true }).first()).toBeVisible();
      const rows = rosterB.getByRole("listitem");
      const selfRow = rows.filter({ hasText: "You" });
      const ownerRow = rows.filter({ hasText: topology.owner.displayName });
      await expect(ownerRow.getByText("2 active sessions", { exact: true })).toBeVisible();
      await expect(selfRow.getByText(/active sessions/)).toHaveCount(0);
      const rosterText = await rosterB.innerText();
      expect(rosterText).not.toMatch(
        /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/i,
      );
      await b.page.addStyleTag({
        content:
          "*,*::before,*::after{animation-duration:0s!important;caret-color:transparent!important;transition-duration:0s!important}",
      });
      await expect(b.page).toHaveScreenshot("presence-roster-light-desktop.png", {
        animations: "disabled",
        caret: "hide",
        maxDiffPixelRatio: 0.005,
        threshold: 0.15,
      });

      const beforeOperation = await durableOperationCount(topology.apiA.pool, topology.boardId);
      await a.page.getByTestId("add-rectangle").click();
      await expect
        .poll(() => durableOperationCount(topology.apiA.pool, topology.boardId))
        .toBe(beforeOperation + 1);
      await ready(a.page);
      // Presence traffic below must not create durable operations.
      const afterDurableOperation = await durableOperationCount(
        topology.apiA.pool,
        topology.boardId,
      );

      const beforeCursor = await canvasDigest(b.page);
      await a.page.locator(".canvas-shell").hover({ position: { x: 120, y: 100 } });
      await expect
        .poll(() =>
          evidenceB.upserts.some(
            (event) =>
              event.participant.userId === topology.owner.id &&
              event.participant.cursor?.x === 120 &&
              event.participant.cursor?.y === 100,
          ),
        )
        .toBe(true);
      await expect.poll(() => canvasDigest(b.page)).not.toBe(beforeCursor);
      expect(await durableOperationCount(topology.apiA.pool, topology.boardId)).toBe(
        afterDurableOperation,
      );
      // Wheel and pan change B's viewport while the target remains a world-coordinate cursor.
      await b.page.locator(".canvas-shell").hover({ position: { x: 220, y: 180 } });
      await b.page.mouse.wheel(0, -120);
      await b.page.getByRole("button", { name: "Pan tool" }).click();
      await b.page.mouse.move(260, 220);
      await b.page.mouse.down();
      await b.page.mouse.move(300, 250);
      await b.page.mouse.up();
      await expect(b.page.locator(".zoom-pill")).toContainText("110%");
      await a.page.locator(".canvas-shell").hover({ position: { x: 140, y: 120 } });
      await expect
        .poll(() =>
          evidenceB.upserts.some(
            (event) =>
              event.participant.userId === topology.owner.id &&
              event.participant.cursor?.x === 140 &&
              event.participant.cursor?.y === 120,
          ),
        )
        .toBe(true);
      await a.page.locator(".canvas-shell").dispatchEvent("pointerleave");
      await expect
        .poll(() =>
          evidenceB.upserts.some(
            (event) =>
              event.participant.userId === topology.owner.id && event.participant.cursor === null,
          ),
        )
        .toBe(true);

      // Presence-only Redis interruption leaves API A's durable editing and synchronization untouched.
      const beforeOutageOperation = await durableOperationCount(
        topology.apiA.pool,
        topology.boardId,
      );
      await topology.interruptApiAPresence();
      await expect(
        a.page.getByRole("button", { name: "Collaborators: Presence temporarily unavailable" }),
      ).toBeVisible();
      await ready(a.page);
      await a.page.getByTestId("add-sticky").click();
      await expect
        .poll(() => durableOperationCount(topology.apiA.pool, topology.boardId))
        .toBe(beforeOutageOperation + 1);
      await expect(
        b.page.getByRole("button", { name: /^Collaborators: [0-9]+ live collaborator/ }),
      ).toBeVisible();
      await expect.poll(() => evidenceA.snapshots.length).toBeGreaterThan(1);
      await expect(
        a.page.getByRole("button", { name: /^Collaborators: / }),
      ).not.toHaveAccessibleName("Collaborators: Presence temporarily unavailable");
      const recoveredSelf = evidenceA.snapshots.at(-1)!;
      expect(recoveredSelf.selfPresenceSessionId).not.toBe(aSelf.selfPresenceSessionId);

      await a.page.locator(".canvas-shell").hover({ position: { x: 160, y: 140 } });
      await expect
        .poll(() =>
          evidenceB.upserts.some(
            (event) =>
              event.participant.userId === topology.owner.id &&
              event.participant.cursor?.x === 160 &&
              event.participant.cursor?.y === 140,
          ),
        )
        .toBe(true);
      await ready(a.page);

      // Native keyboard focus/escape and all three acceptance viewports remain usable.
      const triggerA = a.page.getByRole("button", { name: /^Collaborators:/ });
      await triggerA.focus();
      await a.page.keyboard.press("Enter");
      await expect(a.page.getByRole("region", { name: "Live collaborators" })).toBeVisible();
      await a.page.keyboard.press("Escape");
      await expect(triggerA).toBeFocused();
      await a.page.setViewportSize({ width: 1024, height: 768 });
      await expect
        .poll(() => a.page.evaluate(() => document.documentElement.scrollWidth <= innerWidth))
        .toBe(true);
      await a.page.setViewportSize({ width: 390, height: 844 });
      await expect(a.page.getByRole("button", { name: /^Collaborators:/ })).toBeVisible();
      await expect
        .poll(() => a.page.evaluate(() => document.documentElement.scrollWidth <= innerWidth))
        .toBe(true);
      await a.page.emulateMedia({ forcedColors: "active", reducedMotion: "reduce" });
      await expect(triggerA).toBeVisible();

      await a2.context.close();
      await expect
        .poll(async () => (await roster(b.page)).getByText("2 active sessions").count())
        .toBe(0);
      await b.context.close();
      const rosterA = await roster(a.page);
      await expect(rosterA.getByText("Only you here.", { exact: true })).toBeVisible();
      expect(pageErrors.map((error) => error.message)).toEqual([]);
      await a.context.close();
    } finally {
      await topology.stop();
    }
  });
});
