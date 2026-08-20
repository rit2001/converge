"use client";

import dynamic from "next/dynamic";
import Link from "next/link";
import { useEffect, useMemo, useRef, useState } from "react";
import { visibleObjects } from "@converge/canvas-engine";
import {
  boardSnapshotSchema,
  protocolErrorSchema,
  type BoardSnapshot,
  type DurableCommand,
} from "@converge/protocol";
import {
  BoardSessionController,
  type BoardSessionHandle,
  type BoardSessionToken,
} from "../board-session";
import { useBoardStore } from "../board-store";
import { normalizeRotation } from "../canvas/rotation";
import { indexedDbPendingOperationStore } from "../pending-db";
import { scheduleOwnedSessionStart } from "../owned-session-start";
import { API_URL, BoardTransport, SynchronizationError } from "../transport";
import { IconButton, Separator, StatusPill, Tooltip } from "./ui/primitives";
import { LayersPanel } from "./layers-panel";
import { RotationControls } from "./rotation-controls";
import { toneForSynchronization, WorkspaceEntryStatus } from "./workspace-entry-status";

const Canvas = dynamic(() => import("./canvas").then((module) => module.Canvas), { ssr: false });

function commandBase(boardId: string, clientId: string, targetId: string, lastSeq: number) {
  return {
    schemaVersion: 1 as const,
    opId: crypto.randomUUID(),
    boardId,
    clientId,
    baseSeq: lastSeq,
    targetId,
    clientTimestamp: new Date().toISOString(),
  };
}

function ToolIcon({
  name,
}: {
  name: "select" | "pan" | "rectangle" | "sticky" | "delete" | "layers";
}) {
  const shared = { fill: "none", stroke: "currentColor", strokeWidth: 1.75 };
  if (name === "select")
    return (
      <svg aria-hidden="true" viewBox="0 0 24 24">
        <path {...shared} d="m5 3 14 8-7 2-3 7-4-17Z" />
      </svg>
    );
  if (name === "pan")
    return (
      <svg aria-hidden="true" viewBox="0 0 24 24">
        <path
          {...shared}
          d="M8 12V6a2 2 0 0 1 4 0v5m0 1V4a2 2 0 0 1 4 0v8m0 0V7a2 2 0 0 1 4 0v8c0 4-3 6-7 6h-2c-2 0-3-1-4-3l-2-4a2 2 0 0 1 3-2l2 2"
        />
      </svg>
    );
  if (name === "rectangle")
    return (
      <svg aria-hidden="true" viewBox="0 0 24 24">
        <rect {...shared} x="4" y="6" width="16" height="12" rx="2" />
      </svg>
    );
  if (name === "sticky")
    return (
      <svg aria-hidden="true" viewBox="0 0 24 24">
        <path {...shared} d="M6 4h12v12l-4 4H6V4Z" />
        <path {...shared} d="M14 20v-4h4M9 9h6M9 13h4" />
      </svg>
    );
  if (name === "layers")
    return (
      <svg aria-hidden="true" viewBox="0 0 24 24">
        <path {...shared} d="M5 7h14M5 12h14M5 17h14" />
        <circle {...shared} cx="7" cy="7" r="1" />
        <circle {...shared} cx="7" cy="12" r="1" />
        <circle {...shared} cx="7" cy="17" r="1" />
      </svg>
    );
  return (
    <svg aria-hidden="true" viewBox="0 0 24 24">
      <path {...shared} d="M5 7h14M10 11v6m4-6v6M9 7l1-3h4l1 3m-9 0 1 13h10l1-13" />
    </svg>
  );
}

export function Workspace(): React.JSX.Element {
  const store = useBoardStore();
  const clientId = useMemo(() => crypto.randomUUID(), []);
  const session = useRef<BoardSessionHandle | null>(null);
  const sessions = useMemo(() => {
    const loadSnapshot = async (boardId: string, signal: AbortSignal): Promise<BoardSnapshot> => {
      const response = await fetch(`${API_URL}/v1/boards/${boardId}`, { signal });
      const raw: unknown = await response.json();
      if (!response.ok) {
        const failure = protocolErrorSchema.safeParse(raw);
        if (failure.success)
          throw new SynchronizationError(
            failure.data.code,
            failure.data.message,
            failure.data.retryable,
          );
        if (response.status === 401)
          throw new SynchronizationError(
            "AUTHENTICATION_REQUIRED",
            "Authentication required",
            false,
          );
        if (response.status === 403 || response.status === 404)
          throw new SynchronizationError("FORBIDDEN", "Board is not available", false);
        throw new Error("Could not load board");
      }
      return boardSnapshotSchema.parse(raw);
    };
    return new BoardSessionController({
      store: useBoardStore.getState(),
      createBoard: async (signal): Promise<BoardSnapshot> => {
        const response = await fetch(`${API_URL}/v1/boards`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ name: "Product workshop" }),
          signal,
        });
        if (!response.ok) throw new Error("Could not create board");
        return (await response.json()) as BoardSnapshot;
      },
      loadSnapshot,
      loadPending: async (boardId) => {
        try {
          return await indexedDbPendingOperationStore.load(boardId);
        } catch {
          throw new Error("LOCAL_PERSISTENCE_ERROR: Pending storage is unavailable");
        }
      },
      updateBoardLocation: (boardId) => window.history.replaceState({}, "", `?board=${boardId}`),
      createTransport: (boardId: string, token: BoardSessionToken) =>
        new BoardTransport(boardId, clientId, token, {
          pendingStore: indexedDbPendingOperationStore,
        }),
    });
  }, [clientId]);
  const [tool, setTool] = useState<"select" | "pan">("select");
  const [diagnostics, setDiagnostics] = useState(false);
  const [layersOpen, setLayersOpen] = useState(false);
  const layersTrigger = useRef<HTMLButtonElement>(null);
  const rotationControlsHadFocus = useRef(false);
  const selectedObjectLocked = Boolean(
    store.selectedId && store.lockedObjectIds.has(store.selectedId),
  );
  const selectedObject = useMemo(
    () => store.objects.find((object) => object.id === store.selectedId),
    [store.objects, store.selectedId],
  );
  const rotationAvailable = Boolean(
    selectedObject &&
      store.boardId &&
      store.connection === "ready" &&
      !store.hiddenObjectIds.has(selectedObject.id) &&
      !store.lockedObjectIds.has(selectedObject.id),
  );
  const hasLocalViewControls = store.hiddenObjectIds.size > 0 || store.lockedObjectIds.size > 0;

  const closeLayers = (): void => {
    setLayersOpen(false);
    requestAnimationFrame(() => layersTrigger.current?.focus());
  };

  useEffect(() => {
    return scheduleOwnedSessionStart(
      () => {
        const boardId = new URLSearchParams(window.location.search).get("board");
        const handle = sessions.start(boardId);
        session.current = handle;
        return handle;
      },
      (handle) => {
        sessions.stop(handle);
        if (session.current === handle) session.current = null;
      },
    );
  }, [sessions]);

  const submit = (command: DurableCommand): Promise<boolean> =>
    session.current?.submit(command) ?? Promise.resolve(false);
  const addObject = (kind: "rectangle" | "sticky"): void => {
    if (!store.boardId) return;
    const targetId = crypto.randomUUID();
    const position = {
      x: 120 + store.objects.length * 24,
      y: 100 + store.objects.length * 20,
    };
    const payload =
      kind === "sticky"
        ? {
            id: targetId,
            kind,
            ...position,
            width: 200,
            height: 160,
            rotation: 0,
            fill: "#fde68a",
            text: "New note",
          }
        : {
            id: targetId,
            kind,
            ...position,
            width: 180,
            height: 110,
            rotation: 0,
            fill: "#818cf8",
            text: "" as const,
          };
    const activeSession = session.current;
    void submit({
      ...commandBase(store.boardId, clientId, targetId, store.committed.lastSeq),
      type: "object.create",
      payload,
    }).then((persisted) => {
      if (persisted && session.current === activeSession) store.select(targetId);
    });
  };
  const transform = (
    targetId: string,
    payload: { x?: number; y?: number; width?: number; height?: number; rotation?: number },
  ): void => {
    if (!store.boardId || store.lockedObjectIds.has(targetId)) return;
    void submit({
      ...commandBase(store.boardId, clientId, targetId, store.committed.lastSeq),
      type: "object.transform",
      payload,
    });
  };
  const rotateSelected = (rotation: number): void => {
    if (!rotationAvailable || !selectedObject || store.selectedId !== selectedObject.id) return;
    transform(selectedObject.id, { rotation: normalizeRotation(rotation) });
  };
  const remove = (): void => {
    if (!store.boardId || !store.selectedId || store.lockedObjectIds.has(store.selectedId)) return;
    const targetId = store.selectedId;
    store.select(null);
    void submit({
      ...commandBase(store.boardId, clientId, targetId, store.committed.lastSeq),
      type: "object.delete",
      payload: {},
    });
  };

  useEffect(() => {
    const keydown = (event: KeyboardEvent): void => {
      if (
        event.target instanceof HTMLInputElement ||
        event.target instanceof HTMLTextAreaElement ||
        event.target instanceof HTMLSelectElement
      )
        return;
      if (
        (event.key === "Delete" || event.key === "Backspace") &&
        store.selectedId &&
        !store.lockedObjectIds.has(store.selectedId)
      ) {
        event.preventDefault();
        remove();
      }
      if (event.key === "v") setTool("select");
      if (event.key === "h") setTool("pan");
    };
    window.addEventListener("keydown", keydown);
    return () => window.removeEventListener("keydown", keydown);
  }, [store.selectedId, store.boardId, store.committed.lastSeq, store.lockedObjectIds]);

  useEffect(() => {
    const keydown = (event: KeyboardEvent): void => {
      if (event.key === "Escape" && layersOpen) {
        event.preventDefault();
        closeLayers();
      } else if (event.key === "Escape" && diagnostics) {
        event.preventDefault();
        setDiagnostics(false);
      }
    };
    window.addEventListener("keydown", keydown);
    return () => window.removeEventListener("keydown", keydown);
  }, [diagnostics, layersOpen]);

  useEffect(() => {
    if (rotationAvailable || !rotationControlsHadFocus.current) return;
    rotationControlsHadFocus.current = false;
    requestAnimationFrame(() => layersTrigger.current?.focus());
  }, [rotationAvailable]);

  return (
    <main className="workspace studio-shell" aria-label="Converge studio">
      <header className="topbar studio-board-header" aria-label="Board header">
        <Link className="brand" href="/" aria-label="Converge home">
          <span className="brand-mark" aria-hidden="true">
            C
          </span>
          <span className="brand-name">Converge</span>
        </Link>
        <div className="board-label" aria-label={`Board: ${store.name || "Preparing board"}`}>
          <span>Board</span>
          <strong>{store.name || "Preparing board"}</strong>
        </div>
        <div className="board-header-status">
          <StatusPill
            className="connection"
            label={store.connection}
            tone={toneForSynchronization(store.connection)}
            accessibleLabel={`Synchronization status: ${store.connection}`}
          />
          <Tooltip label="Open layers">
            <button
              ref={layersTrigger}
              className={`ui-button ui-button--${layersOpen ? "primary" : "secondary"} ui-button--icon layers-trigger`}
              type="button"
              aria-label="Open layers panel"
              aria-expanded={layersOpen}
              aria-controls="layers-panel"
              onClick={() => setLayersOpen((value) => !value)}
            >
              <ToolIcon name="layers" />
            </button>
          </Tooltip>
        </div>
      </header>
      <aside className="toolbar studio-tool-dock" aria-label="Primary canvas tools">
        <div role="toolbar" aria-label="Canvas tools">
          <Tooltip label="Select (V)">
            <IconButton
              className="workspace-tool-button"
              variant={tool === "select" ? "primary" : "ghost"}
              aria-label="Select tool"
              aria-pressed={tool === "select"}
              aria-keyshortcuts="V"
              onClick={() => setTool("select")}
            >
              <ToolIcon name="select" />
            </IconButton>
          </Tooltip>
          <Tooltip label="Pan (H)">
            <IconButton
              className="workspace-tool-button"
              variant={tool === "pan" ? "primary" : "ghost"}
              aria-label="Pan tool"
              aria-pressed={tool === "pan"}
              aria-keyshortcuts="H"
              onClick={() => setTool("pan")}
            >
              <ToolIcon name="pan" />
            </IconButton>
          </Tooltip>
          <Separator />
          <Tooltip label="Add rectangle">
            <IconButton
              className="workspace-tool-button"
              variant="ghost"
              aria-label="Add rectangle"
              data-testid="add-rectangle"
              onClick={() => addObject("rectangle")}
            >
              <ToolIcon name="rectangle" />
            </IconButton>
          </Tooltip>
          <Tooltip label="Add sticky note">
            <IconButton
              className="workspace-tool-button"
              variant="ghost"
              aria-label="Add sticky note"
              data-testid="add-sticky"
              onClick={() => addObject("sticky")}
            >
              <ToolIcon name="sticky" />
            </IconButton>
          </Tooltip>
          <Separator />
          {rotationAvailable && selectedObject && (
            <RotationControls
              object={selectedObject}
              onRotate={rotateSelected}
              onFocusWithinChange={(focused) => {
                rotationControlsHadFocus.current = focused;
              }}
            />
          )}
          {rotationAvailable && selectedObject && <Separator />}
          <Tooltip label="Delete selected">
            <IconButton
              className="workspace-tool-button"
              variant="ghost"
              aria-label="Delete selected"
              disabled={!store.selectedId || selectedObjectLocked}
              onClick={remove}
            >
              <ToolIcon name="delete" />
            </IconButton>
          </Tooltip>
        </div>
      </aside>
      <section id="studio-canvas-region" className="studio-canvas-region" aria-label="Board canvas">
        <Canvas
          objects={store.objects}
          selectedId={store.selectedId}
          hiddenObjectIds={store.hiddenObjectIds}
          lockedObjectIds={store.lockedObjectIds}
          tool={tool}
          rotationEnabled={rotationAvailable}
          rotationFence={`${store.sessionGeneration ?? "none"}:${store.connection}`}
          onSelect={(id) => store.select(id)}
          onTransform={transform}
        />
      </section>
      {layersOpen && (
        <LayersPanel
          objects={store.objects}
          selectedId={store.selectedId}
          hiddenObjectIds={store.hiddenObjectIds}
          lockedObjectIds={store.lockedObjectIds}
          onSelect={(id) => store.select(id)}
          onToggleHidden={(id) => store.setObjectHidden(id, !store.hiddenObjectIds.has(id))}
          onToggleLocked={(id) => store.setObjectLocked(id, !store.lockedObjectIds.has(id))}
          onClose={closeLayers}
        />
      )}
      <output className="ui-visually-hidden" aria-live="polite" aria-atomic="true">
        {store.selectionNotice}
      </output>
      <WorkspaceEntryStatus status={store.connection} hasBoard={Boolean(store.boardId)} />
      {store.error && (
        <div className="error-toast" role="alert">
          {store.error}
        </div>
      )}
      <aside className="studio-narrow-notice" aria-label="Desktop editor notice">
        <strong>Desktop-first studio</strong>
        <span>Canvas editing is optimized for a larger screen.</span>
        {hasLocalViewControls && (
          <button
            className="studio-narrow-reset"
            type="button"
            onClick={() => store.clearLocalViewControls()}
          >
            Restore local layers
          </button>
        )}
      </aside>
      <section className={`diagnostics studio-system-details ${diagnostics ? "open" : ""}`}>
        <button
          type="button"
          aria-expanded={diagnostics}
          aria-controls="workspace-diagnostics-panel"
          onClick={() => setDiagnostics((value) => !value)}
        >
          <span>Diagnostics</span>
          <b aria-hidden="true">{diagnostics ? "−" : "+"}</b>
        </button>
        <dl id="workspace-diagnostics-panel" hidden={!diagnostics}>
          <div>
            <dt>Board</dt>
            <dd data-testid="board-id">{store.boardId ?? "none"}</dd>
          </div>
          <div>
            <dt>Connection</dt>
            <dd>{store.connection}</dd>
          </div>
          <div>
            <dt>Last sequence</dt>
            <dd data-testid="last-seq">{store.committed.lastSeq}</dd>
          </div>
          <div>
            <dt>Pending</dt>
            <dd data-testid="pending-count">{store.pending.length}</dd>
          </div>
          <div>
            <dt>Pending recovery</dt>
            <dd data-testid="pending-status">{store.pendingStatus}</dd>
          </div>
          <div>
            <dt>Sync attempt</dt>
            <dd data-testid="sync-attempt">{store.synchronizationDiagnostics.attempt}</dd>
          </div>
          <div>
            <dt>Sync retry</dt>
            <dd data-testid="sync-retry-code">
              {store.synchronizationDiagnostics.retryScheduled
                ? `${store.synchronizationDiagnostics.retryCode ?? "retry"} (${store.synchronizationDiagnostics.retryDelayMs ?? 0}ms)`
                : "none"}
            </dd>
          </div>
          <div>
            <dt>Sync buffer</dt>
            <dd data-testid="sync-buffer">
              {store.synchronizationDiagnostics.bufferedCount}/
              {store.synchronizationDiagnostics.bufferCountLimit} operations,{" "}
              {store.synchronizationDiagnostics.bufferedBytes}/
              {store.synchronizationDiagnostics.bufferByteLimit} bytes
            </dd>
          </div>
          <div className="hash">
            <dt>Committed objects</dt>
            <dd data-testid="committed-objects">
              {JSON.stringify(visibleObjects(store.committed))}
            </dd>
          </div>
          <div>
            <dt>Hash board</dt>
            <dd data-testid="hash-board-id">{store.authoritativeHash.boardId ?? "none"}</dd>
          </div>
          <div>
            <dt>Hash sequence</dt>
            <dd data-testid="hash-seq">{store.authoritativeHash.seq ?? "none"}</dd>
          </div>
          <div>
            <dt>Hash session</dt>
            <dd data-testid="hash-session-generation">
              {store.authoritativeHash.sessionGeneration ?? "none"}
            </dd>
          </div>
          <div>
            <dt>Hash status</dt>
            <dd data-testid="hash-status">{store.authoritativeHash.status}</dd>
          </div>
          <div className="hash">
            <dt>Authoritative hash</dt>
            <dd data-testid="state-hash">{store.authoritativeHash.value ?? "unavailable"}</dd>
          </div>
        </dl>
      </section>
    </main>
  );
}
