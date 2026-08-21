export type MicrotaskScheduler = (callback: () => void) => void;

export function scheduleOwnedSessionStart<Handle>(
  start: () => Handle,
  stop: (handle: Handle) => void,
  schedule: MicrotaskScheduler = queueMicrotask,
): () => void {
  let disposed = false;
  let handle: Handle | null = null;

  schedule(() => {
    if (disposed) return;
    handle = start();
  });

  return () => {
    if (disposed) return;
    disposed = true;
    if (handle !== null) stop(handle);
  };
}
