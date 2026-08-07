"use client";

import dynamic from "next/dynamic";
import { useEffect, useMemo, useRef, useState } from "react";
import type { BoardSnapshot, DurableCommand } from "@converge/protocol";
import { useBoardStore } from "../board-store";
import { loadPending } from "../pending-db";
import { API_URL, BoardTransport } from "../transport";

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
  const transport = useRef<BoardTransport | null>(null);
  const [tool, setTool] = useState<"select" | "pan">("select");
  const [diagnostics, setDiagnostics] = useState(true);

  useEffect(() => {
    let active = true;
    const start = async (): Promise<void> => {
      const query = new URLSearchParams(window.location.search);
      let boardId = query.get("board");
      if (!boardId) {
        const created = await fetch(`${API_URL}/v1/boards`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ name: "Product workshop" }),
        });
        if (!created.ok) throw new Error("Could not create board");
        const board = (await created.json()) as BoardSnapshot;
        boardId = board.id;
        window.history.replaceState({}, "", `?board=${boardId}`);
      }
      const response = await fetch(`${API_URL}/v1/boards/${boardId}`);
      if (!response.ok) throw new Error("Could not load board");
      const snapshot = (await response.json()) as BoardSnapshot;
      if (!active) return;
      store.initialize(snapshot, await loadPending(boardId));
      const nextTransport = new BoardTransport(boardId, clientId);
      transport.current = nextTransport;
      nextTransport.connect();
    };
    void start().catch((error: unknown) =>
      useBoardStore.setState({
        error: error instanceof Error ? error.message : "Startup failed",
        connection: "error",
      }),
    );
    return () => {
      active = false;
      transport.current?.disconnect();
    };
  }, [clientId]);

  const submit = (command: DurableCommand): void => {
    store.enqueue(command);
    transport.current?.submit(command);
  };
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
    submit({
      ...commandBase(store.boardId, clientId, targetId, store.committed.lastSeq),
      type: "object.create",
      payload,
    });
    store.select(targetId);
  };
  const transform = (
    targetId: string,
    payload: { x: number; y: number; width?: number; height?: number },
  ): void => {
    if (!store.boardId) return;
    submit({
      ...commandBase(store.boardId, clientId, targetId, store.committed.lastSeq),
      type: "object.transform",
      payload,
    });
  };
  const remove = (): void => {
    if (!store.boardId || !store.selectedId) return;
    submit({
      ...commandBase(store.boardId, clientId, store.selectedId, store.committed.lastSeq),
      type: "object.delete",
      payload: {},
    });
    store.select(null);
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
        <div className="connection">
          <i className={store.connection} />
          {store.connection}
        </div>
        <button className="avatar" title="Development identity">
          LD
        </button>
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
      {store.error && <div className="error-toast">{store.error}</div>}
      <section className={`diagnostics ${diagnostics ? "open" : ""}`}>
        <button onClick={() => setDiagnostics((value) => !value)}>
          <span>Diagnostics</span>
          <b>{diagnostics ? "−" : "+"}</b>
        </button>
        {diagnostics && (
          <dl>
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
            <div className="hash">
              <dt>State hash</dt>
              <dd data-testid="state-hash">{store.hash}</dd>
            </div>
          </dl>
        )}
      </section>
    </main>
  );
}
