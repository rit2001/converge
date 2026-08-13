import * as React from "react";

const roots = [
  ["overlay-popovers", "popover"],
  ["overlay-status", "notification"],
  ["overlay-modals", "dialog"],
  ["overlay-terminal", "terminal"],
] as const;

export function PortalRoots(): React.JSX.Element {
  return (
    <div className="portal-hosts">
      {roots.map(([id, layer]) => (
        <div key={id} id={id} className="portal-root" data-layer={layer} />
      ))}
    </div>
  );
}

export { roots as portalRootContract };
