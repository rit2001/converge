"use client";

import dynamic from "next/dynamic";
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
import { indexedDbPendingOperationStore } from "../pending-db";
import { scheduleOwnedSessionStart } from "../owned-session-start";
import { API_URL, BoardTransport, SynchronizationError } from "../transport";
import { Button, StatusPill } from "./ui/primitives";
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
  const [diagnostics, setDiagnostics] = useState(true);

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
    payload: { x: number; y: number; width?: number; height?: number },
  ): void => {
    if (!store.boardId) return;
    void submit({
      ...commandBase(store.boardId, clientId, targetId, store.committed.lastSeq),
      type: "object.transform",
      payload,
    });
  };
  const remove = (): void => {
    if (!store.boardId || !store.selectedId) return;
    const activeSession = session.current;
    void submit({
      ...commandBase(store.boardId, clientId, store.selectedId, store.committed.lastSeq),
      type: "object.delete",
      payload: {},
    }).then((persisted) => {
      if (persisted && session.current === activeSession) store.select(null);
    });
  };

  useEffect(() => {
    const keydown = (event: KeyboardEvent): void => {
      if ((event.key === "Delete" || event.key === "Backspace") && store.selectedId) {
        event.preventDefault();
        remove();
      }
      if (event.key === "v") setTool("select");
      if (event.key === "h") setTool("pan");
    };
    window.addEventListener("keydown", keydown);
    return () => window.removeEventListener("keydown", keydown);
  }, [store.selectedId, store.boardId, store.committed.lastSeq]);

  return (
    <main className="workspace">
      <header className="topbar">
        <div className="brand">
          <span className="brand-mark">C</span>
          <div>
            <strong>Converge</strong>
            <span>{store.name}</span>
          </div>
        </div>
        <StatusPill
          className="connection"
          label={store.connection}
          tone={toneForSynchronization(store.connection)}
          accessibleLabel={`Synchronization status: ${store.connection}`}
        />
        <Button
          variant="ghost"
          className="avatar"
          aria-label="Development identity"
          title="Development identity"
        >
          LD
        </Button>
      </header>
      <aside className="toolbar" aria-label="Canvas tools">
        <button
          className={tool === "select" ? "active" : ""}
          onClick={() => setTool("select")}
          title="Select (V)"
        >
          ↖
        </button>
        <button
          className={tool === "pan" ? "active" : ""}
          onClick={() => setTool("pan")}
          title="Pan (H)"
        >
          ✋
        </button>
        <hr />
        <button
          data-testid="add-rectangle"
          onClick={() => addObject("rectangle")}
          title="Add rectangle"
        >
          ▭
        </button>
        <button
          data-testid="add-sticky"
          onClick={() => addObject("sticky")}
          title="Add sticky note"
        >
          ▤
        </button>
        <hr />
        <button onClick={remove} disabled={!store.selectedId} title="Delete selected">
          ⌫
        </button>
      </aside>
      <Canvas
        objects={store.objects}
        selectedId={store.selectedId}
        tool={tool}
        onSelect={(id) => store.select(id)}
        onTransform={transform}
      />
      <WorkspaceEntryStatus status={store.connection} hasBoard={Boolean(store.boardId)} />
      {store.error && (
        <div className="error-toast" role="alert">
          {store.error}
        </div>
      )}
      <section className={`diagnostics ${diagnostics ? "open" : ""}`}>
        <button onClick={() => setDiagnostics((value) => !value)}>
          <span>Diagnostics</span>
          <b>{diagnostics ? "−" : "+"}</b>
        </button>
        {diagnostics && (
          <dl>
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
        )}
      </section>
    </main>
  );
}
