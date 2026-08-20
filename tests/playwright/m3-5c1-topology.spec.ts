import { expect, test, type Page } from "@playwright/test";
import { readdir } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { startDualReplicaBrowserTopology } from "./dual-replica-topology.js";

type PresenceSnapshotWire = {
  selfPresenceSessionId: string;
  participants: Array<{ presenceSessionId: string; userId: string }>;
};
type PresenceUpsertWire = {
  participant: { cursor: { x: number; y: number } | null; userId: string };
};

async function capturePresence(
  page: Page,
): Promise<{ snapshots: PresenceSnapshotWire[]; upserts: PresenceUpsertWire[] }> {
  const evidence = { snapshots: [] as PresenceSnapshotWire[], upserts: [] as PresenceUpsertWire[] };
  const session = await page.context().newCDPSession(page);
  await session.send("Network.enable");
  session.on("Network.webSocketFrameReceived", ({ response }) => {
    const payload = response.payloadData;
    if (!payload.startsWith("42")) return;
    try {
      const [event, value] = JSON.parse(payload.slice(2)) as [string, unknown];
      if (event === "board:presence-snapshot")
        evidence.snapshots.push(value as PresenceSnapshotWire);
      if (event === "presence:participant-upsert")
        evidence.upserts.push(value as PresenceUpsertWire);
    } catch {
      /* bounded wire evidence only */
    }
  });
  return evidence;
}

async function ready(page: Page): Promise<void> {
  await expect(page.getByRole("button", { name: "Synchronization status: Synced" })).toBeVisible();
}

test.describe.serial("M3.5C1 dual-replica browser topology", () => {
  test("routes real browser sessions through independent web/API replicas", async ({ browser }) => {
    const topology = await startDualReplicaBrowserTopology();
    try {
      const a = await topology.openA(browser);
      const a2 = await topology.openA(browser);
      const b = await topology.openB(browser);
      const evidenceA = await capturePresence(a.page);
      const evidenceA2 = await capturePresence(a2.page);
      const evidenceB = await capturePresence(b.page);
      await Promise.all([a.page.reload(), a2.page.reload(), b.page.reload()]);
      await Promise.all([ready(a.page), ready(a2.page), ready(b.page)]);
      expect(new URL(a.page.url()).origin).toBe(topology.webA.origin);
      expect(new URL(b.page.url()).origin).toBe(topology.webB.origin);
      expect(topology.webA.origin).not.toBe(topology.webB.origin);
      expect(topology.apiA.origin).not.toBe(topology.apiB.origin);
      await expect.poll(() => evidenceA.snapshots.length).toBeGreaterThan(0);
      await expect.poll(() => evidenceA2.snapshots.length).toBeGreaterThan(0);
      await expect.poll(() => evidenceB.snapshots.length).toBeGreaterThan(0);
      const aSnapshot = evidenceA.snapshots.at(-1)!;
      const a2Snapshot = evidenceA2.snapshots.at(-1)!;
      const bSnapshot = evidenceB.snapshots.at(-1)!;
      expect(aSnapshot.selfPresenceSessionId).not.toBe(a2Snapshot.selfPresenceSessionId);
      expect(
        aSnapshot.participants.find(
          (item) => item.presenceSessionId === aSnapshot.selfPresenceSessionId,
        )?.userId,
      ).toBe(topology.owner.id);
      expect(
        a2Snapshot.participants.find(
          (item) => item.presenceSessionId === a2Snapshot.selfPresenceSessionId,
        )?.userId,
      ).toBe(topology.owner.id);
      expect(
        bSnapshot.participants.find(
          (item) => item.presenceSessionId === bSnapshot.selfPresenceSessionId,
        )?.userId,
      ).toBe(topology.editor.id);
      await a.page.locator(".canvas-shell").hover({ position: { x: 120, y: 100 } });
      await expect
        .poll(() =>
          evidenceB.upserts.some(
            (event) =>
              event.participant.userId === topology.owner.id && event.participant.cursor !== null,
          ),
        )
        .toBe(true);
      await Promise.all([a.context.close(), a2.context.close(), b.context.close()]);
    } finally {
      await topology.stop();
    }
  });

  test("cleans owned resources after a forced partial startup failure", async () => {
    await expect(startDualReplicaBrowserTopology({ failAfterApiA: true })).rejects.toThrow(
      "Test topology forced startup failure",
    );
    const webRoot = fileURLToPath(new URL("../../apps/web/", import.meta.url));
    expect((await readdir(webRoot)).filter((entry) => entry.startsWith(".next-m35c-"))).toEqual([]);
  });
});
