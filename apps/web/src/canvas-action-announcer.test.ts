import { expect, it } from "vitest";
import { nextCanvasAnnouncement } from "./canvas-action-announcer";
it("increments a bounded generation so equal completed actions remain announceable", () => {
  const first = nextCanvasAnnouncement({ generation: 0, message: "" }, "Object selected.");
  const second = nextCanvasAnnouncement(first, "Object selected.");
  expect(second).toEqual({ generation: 2, message: "Object selected." });
});
