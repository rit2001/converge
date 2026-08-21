"use client";

import Link from "next/link";
import { Button, StatusPill, Surface } from "../../src/components/ui/primitives";

export default function StudioError({
  reset,
}: {
  error: Error;
  reset: () => void;
}): React.JSX.Element {
  return (
    <main className="studio-route-state" aria-labelledby="studio-error-title">
      <Surface className="studio-route-state__panel" role="alert">
        <StatusPill label="Unavailable" tone="unavailable" />
        <h1 id="studio-error-title">The studio could not be prepared</h1>
        <p>No editing session was started. You can try again or return to the introduction.</p>
        <div className="studio-route-state__actions">
          <Button variant="primary" onClick={reset}>
            Try again
          </Button>
          <Link className="ui-button ui-button--secondary ui-button--default" href="/">
            Return home
          </Link>
        </div>
      </Surface>
    </main>
  );
}
