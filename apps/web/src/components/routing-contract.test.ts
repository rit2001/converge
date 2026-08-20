import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";

const read = (path: string): string => readFileSync(resolve(process.cwd(), path), "utf8");

describe("landing and studio route ownership", () => {
  it("keeps the public route isolated from the collaboration bundle and side effects", () => {
    const rootPage = read("app/page.tsx");
    const landing = read("src/components/landing-page.tsx");
    const publicSources = `${rootPage}\n${landing}`;

    expect(rootPage).toContain('from "../src/components/landing-page"');
    for (const forbidden of [
      "Workspace",
      "BoardTransport",
      "BoardStore",
      "fetch(",
      "socket.io",
      "konva",
    ])
      expect(publicSources).not.toContain(forbidden);
  });

  it("owns workspace initialization only from the dedicated studio route", () => {
    const studioPage = read("app/studio/page.tsx");
    const workspace = read("src/components/workspace.tsx");

    expect(studioPage).toContain('from "../../src/components/workspace"');
    expect(workspace.match(/sessions\.start\(/g)).toHaveLength(1);
    expect(workspace.match(/sessions\.stop\(/g)).toHaveLength(1);
    expect(workspace).toContain('new URLSearchParams(window.location.search).get("board")');
    expect(workspace).toContain('window.history.replaceState({}, "", `?board=${boardId}`)');
  });

  it("keeps route-level preparing and failure boundaries owned by studio", () => {
    expect(read("app/studio/loading.tsx")).toContain("Preparing your studio");
    const errorBoundary = read("app/studio/error.tsx");
    expect(errorBoundary).toContain("No editing session was started");
    expect(errorBoundary).toContain('href="/"');
  });
});
