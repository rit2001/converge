import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";
import type { PresenceSnapshot } from "../presence-store";
import { CollaboratorPresence } from "./collaborator-presence";

const presence: PresenceSnapshot = {
  availability: "available",
  current: true,
  selfUserId: "10000000-0000-4000-8000-000000000001",
  selfPresenceSessionId: "20000000-0000-4000-8000-000000000001",
  collaborators: [
    {
      key: "10000000-0000-4000-8000-000000000001",
      label: "You",
      displayName: "Alex Example",
      self: true,
      activity: "active",
      cursor: null,
      paletteToken: "collaborator-1",
      current: true,
      sessionCount: 2,
      activeSessionCount: 2,
    },
    ...["Blair", "Casey", "Devon", "Emery"].map((displayName, index) => ({
      key: `30000000-0000-4000-8000-${String(index).padStart(12, "0")}`,
      label: displayName,
      displayName,
      self: false,
      activity: index === 0 ? ("active" as const) : ("idle" as const),
      cursor: null,
      paletteToken: "collaborator-2" as const,
      current: true,
      sessionCount: 1,
      activeSessionCount: index === 0 ? 1 : 0,
    })),
  ],
};

describe("CollaboratorPresence", () => {
  it("uses grouped, accessible names without exposing internal identities", () => {
    const markup = renderToStaticMarkup(createElement(CollaboratorPresence, { presence }));
    expect(markup).toContain("Collaborators: 4 live collaborators");
    expect(markup).toContain("+1");
    expect(markup).toContain("AE");
    expect(markup).not.toContain(presence.selfUserId!);
    expect(markup).not.toContain(presence.selfPresenceSessionId!);
  });
  it("uses bounded unavailable language rather than fabricating a collaborator", () => {
    const markup = renderToStaticMarkup(
      createElement(CollaboratorPresence, {
        presence: { ...presence, availability: "unavailable", current: false },
      }),
    );
    expect(markup).toContain("Presence temporarily unavailable");
  });
});
