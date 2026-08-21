import { StatusPill, Surface } from "../../src/components/ui/primitives";

export default function StudioLoading(): React.JSX.Element {
  return (
    <main className="studio-route-state" aria-labelledby="studio-loading-title">
      <Surface className="studio-route-state__panel">
        <StatusPill label="Preparing" tone="information" accessibleLabel="Workspace preparing" />
        <h1 id="studio-loading-title">Preparing your studio</h1>
        <p>Setting out the workspace before collaboration begins.</p>
      </Surface>
    </main>
  );
}
