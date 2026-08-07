import { io, type Socket } from "socket.io-client";
import type {
  ClientToServerEvents,
  CommittedOperation,
  DurableCommand,
  OperationAck,
  ServerToClientEvents,
} from "@converge/protocol";
import { firstBufferedGap, useBoardStore } from "./board-store";

const API_URL = process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:4000";

export class BoardTransport {
  private socket: Socket<ServerToClientEvents, ClientToServerEvents> | null = null;
  private recovering = false;
  constructor(
    private readonly boardId: string,
    private readonly clientId: string,
  ) {}

  connect(): void {
    const socket = io(API_URL, { auth: {}, reconnection: true });
    this.socket = socket;
    socket.on("connect", () => {
      const state = useBoardStore.getState();
      state.setConnection("connected");
      socket.emit(
        "board:join",
        {
          schemaVersion: 1,
          boardId: this.boardId,
          clientId: this.clientId,
          lastAppliedSeq: state.committed.lastSeq,
          pendingOpIds: state.pending.map((item) => item.opId),
        },
        (ack: OperationAck | { ok: true }) => {
          if ("ok" in ack && ack.ok)
            for (const command of useBoardStore.getState().pending) this.submit(command);
        },
      );
    });
    socket.on("disconnect", () => useBoardStore.getState().setConnection("reconnecting"));
    socket.on("operation:committed", (operation: CommittedOperation) => {
      useBoardStore.getState().ingest(operation);
      void this.recoverGap();
    });
    socket.io.on("reconnect_attempt", () => useBoardStore.getState().setConnection("reconnecting"));
    socket.io.on("reconnect_failed", () => useBoardStore.getState().setConnection("offline"));
  }

  submit(command: DurableCommand): void {
    if (!this.socket?.connected) return;
    this.socket.emit("operation:submit", command, (ack: OperationAck) =>
      useBoardStore.getState().acknowledge(command.opId, ack),
    );
  }

  disconnect(): void {
    this.socket?.disconnect();
    this.socket = null;
  }

  private async recoverGap(): Promise<void> {
    const gap = firstBufferedGap();
    if (!gap || this.recovering) return;
    this.recovering = true;
    try {
      const response = await fetch(
        `${API_URL}/v1/boards/${this.boardId}/operations?from=${gap.from}&to=${gap.to}`,
      );
      if (!response.ok) throw new Error("Missing operation recovery failed");
      const body = (await response.json()) as { operations: CommittedOperation[] };
      for (const operation of body.operations) useBoardStore.getState().ingest(operation);
    } finally {
      this.recovering = false;
    }
  }
}

export { API_URL };
