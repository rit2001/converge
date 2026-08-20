import * as React from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it, vi } from "vitest";
import Page from "../../app/page";
import { LandingPage } from "./landing-page";

describe("premium landing page", () => {
  it("renders one accessible editorial page structure", () => {
    const markup = renderToStaticMarkup(<LandingPage />);

    expect(markup.match(/<h1/g)).toHaveLength(1);
    expect(markup).toContain("<header");
    expect(markup).toContain('<nav aria-label="Primary navigation"');
    expect(markup).toContain('<main id="main-content"');
    expect(markup).toContain("<section");
    expect(markup).toContain("<footer");
    expect(markup).toContain('class="skip-link" href="#main-content"');
  });

  it("provides native keyboard-operable studio and in-page actions", () => {
    const markup = renderToStaticMarkup(<LandingPage />);

    expect(markup.match(/href="\/studio"/g)?.length).toBeGreaterThanOrEqual(3);
    expect(markup).toContain('href="#trust"');
    expect(markup).not.toContain('role="button"');
  });

  it("keeps the static product illustration outside the accessibility tree", () => {
    const markup = renderToStaticMarkup(<LandingPage />);

    expect(markup).toContain('class="product-illustration" aria-hidden="true"');
    expect(markup).not.toContain("<canvas");
    expect(markup).not.toContain("<video");
  });

  it("states exact accepted evidence with its controlled-local qualification", () => {
    const markup = renderToStaticMarkup(<LandingPage />);

    for (const evidence of [
      "10",
      "concurrent active editors",
      "1,300",
      "acknowledged durable commands",
      "227.03 ms",
      "p99 acknowledgement",
      "368 ms",
      "p99 live delivery",
      "zero protocol failures, sequence gaps, or logical reapplications",
      "controlled local baseline—not a production capacity claim",
    ]) {
      expect(markup).toContain(evidence);
    }
  });

  it("contains no unsupported capacity or delivery claims", () => {
    const copy = renderToStaticMarkup(<LandingPage />).toLowerCase();

    for (const unsupported of [
      "exactly-once",
      "exactly once",
      "10,000",
      "enterprise scale",
      "unlimited users",
      "production ready",
    ]) {
      expect(copy).not.toContain(unsupported);
    }
  });

  it("renders the root route without board, API, or socket initialization", () => {
    const fetchSpy = vi.spyOn(globalThis, "fetch");

    const markup = renderToStaticMarkup(<Page />);

    expect(markup).toContain("Shared thinking that survives the network");
    expect(fetchSpy).not.toHaveBeenCalled();
    fetchSpy.mockRestore();
  });
});
