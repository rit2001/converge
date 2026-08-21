import * as React from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";
import {
  Button,
  IconButton,
  Separator,
  StatusPill,
  Surface,
  Tooltip,
  VisuallyHidden,
} from "./primitives";
import { PortalRoots, portalRootContract } from "./portal-roots";

describe("premium UI primitives", () => {
  it.each(["primary", "secondary", "ghost", "danger"] as const)(
    "renders the %s button variant as a native keyboard-operable control",
    (variant) => {
      const markup = renderToStaticMarkup(<Button variant={variant}>Continue</Button>);

      expect(markup).toContain('<button type="button"');
      expect(markup).toContain(`ui-button--${variant}`);
      expect(markup).toContain(">Continue</button>");
    },
  );

  it("exposes disabled and loading state without changing the accessible label", () => {
    const markup = renderToStaticMarkup(<Button loading>Save changes</Button>);

    expect(markup).toContain("disabled");
    expect(markup).toContain('aria-busy="true"');
    expect(markup).toContain("Save changes");
  });

  it("requires an accessible name for icon-only controls", () => {
    expect(() => renderToStaticMarkup(<IconButton aria-label="">+</IconButton>)).toThrow(
      "IconButton requires a nonempty accessible name",
    );

    const markup = renderToStaticMarkup(<IconButton aria-label="Add object">+</IconButton>);
    expect(markup).toContain('aria-label="Add object"');
    expect(markup).toContain("ui-button--icon");
  });

  it("communicates status with visible text and semantics, not color alone", () => {
    const markup = renderToStaticMarkup(
      <StatusPill label="recovering" tone="recovering" accessibleLabel="Sync status: recovering" />,
    );

    expect(markup).toContain('role="status"');
    expect(markup).toContain('aria-label="Sync status: recovering"');
    expect(markup).toContain(">recovering</span>");
  });

  it("provides semantic structural and descriptive primitives", () => {
    const markup = renderToStaticMarkup(
      <Surface aria-label="Inspector">
        <VisuallyHidden>Hidden context</VisuallyHidden>
        <Separator />
        <Tooltip label="Create shape">
          <button type="button">Shape</button>
        </Tooltip>
      </Surface>,
    );

    expect(markup).toContain('<section aria-label="Inspector"');
    expect(markup).toContain("ui-visually-hidden");
    expect(markup).toContain("ui-separator");
    expect(markup).toContain('aria-describedby="');
    expect(markup).toContain('role="tooltip"');
  });

  it("renders the four portal hosts once in ADR order", () => {
    const markup = renderToStaticMarkup(<PortalRoots />);

    expect(portalRootContract).toEqual([
      ["overlay-popovers", "popover"],
      ["overlay-status", "notification"],
      ["overlay-modals", "dialog"],
      ["overlay-terminal", "terminal"],
    ]);
    for (const [id] of portalRootContract) {
      expect(markup.match(new RegExp(`id="${id}"`, "g"))).toHaveLength(1);
    }
    expect(markup.indexOf("overlay-popovers")).toBeLessThan(markup.indexOf("overlay-status"));
    expect(markup.indexOf("overlay-status")).toBeLessThan(markup.indexOf("overlay-modals"));
    expect(markup.indexOf("overlay-modals")).toBeLessThan(markup.indexOf("overlay-terminal"));
    expect(markup).not.toContain("aria-hidden");
  });

  it("uses the native disabled contract rather than hover-only behavior", () => {
    const markup = renderToStaticMarkup(<Button disabled>Unavailable</Button>);

    expect(markup).toContain("disabled");
    expect(markup).not.toContain("aria-disabled");
  });
});
