import type { CanvasObject, CommittedOperation, DurableCommand } from "@converge/protocol";

export interface ProjectedObject {
  value: CanvasObject;
  createdSeq: number;
  updatedSeq: number;
  deletedSeq: number | null;
  fieldSeq: Record<string, number>;
}

export interface BoardState {
  lastSeq: number;
  objects: Record<string, ProjectedObject>;
  order: string[];
}

export type ReductionResult =
  | { ok: true; state: BoardState }
  | { ok: false; code: "TARGET_NOT_FOUND" | "TARGET_DELETED" | "CONFLICT"; message: string };

export const emptyBoardState = (): BoardState => ({ lastSeq: 0, objects: {}, order: [] });

function activeObject(state: BoardState, targetId: string): ProjectedObject | undefined {
  const projected = state.objects[targetId];
  return projected?.deletedSeq === null ? projected : undefined;
}

function updateFields(projected: ProjectedObject, payload: object, seq: number): ProjectedObject {
  const value = { ...projected.value } as CanvasObject;
  const fieldSeq = { ...projected.fieldSeq };
  const entries = Object.entries(payload) as [string, unknown][];
  for (const [field, next] of entries) {
    if (typeof next !== "string" && typeof next !== "number") continue;
    const patch: Record<string, string | number> = { [field]: next };
    Object.assign(value, patch);
    fieldSeq[field] = seq;
  }
  return { ...projected, value, fieldSeq, updatedSeq: seq };
}

export function reduceCommand(
  state: BoardState,
  command: DurableCommand,
  seq: number,
): ReductionResult {
  if (command.type === "object.create") {
    const existing = state.objects[command.targetId];
    if (existing) return { ok: false, code: "CONFLICT", message: "Target id already exists" };
    if (command.payload.id !== command.targetId) {
      return { ok: false, code: "CONFLICT", message: "Payload id must match target id" };
    }
    const fieldSeq = Object.fromEntries(Object.keys(command.payload).map((field) => [field, seq]));
    return {
      ok: true,
      state: {
        lastSeq: seq,
        objects: {
          ...state.objects,
          [command.targetId]: {
            value: command.payload,
            createdSeq: seq,
            updatedSeq: seq,
            deletedSeq: null,
            fieldSeq,
          },
        },
        order: [...state.order, command.targetId],
      },
    };
  }

  const historical = state.objects[command.targetId];
  if (!historical) return { ok: false, code: "TARGET_NOT_FOUND", message: "Target does not exist" };
  if (historical.deletedSeq !== null) {
    return {
      ok: false,
      code: "TARGET_DELETED",
      message: "Delete wins over later or stale updates",
    };
  }
  const projected = activeObject(state, command.targetId);
  if (!projected) return { ok: false, code: "TARGET_DELETED", message: "Target is deleted" };

  if (command.type === "object.delete") {
    return {
      ok: true,
      state: {
        lastSeq: seq,
        objects: {
          ...state.objects,
          [command.targetId]: { ...projected, deletedSeq: seq, updatedSeq: seq },
        },
        order: state.order.filter((id) => id !== command.targetId),
      },
    };
  }

  const next = updateFields(projected, command.payload, seq);
  return {
    ok: true,
    state: { ...state, lastSeq: seq, objects: { ...state.objects, [command.targetId]: next } },
  };
}

export function applyCommitted(state: BoardState, operation: CommittedOperation): BoardState {
  if (operation.seq <= state.lastSeq) return state;
  if (operation.seq !== state.lastSeq + 1)
    throw new Error("Committed operations must be applied in order");
  const result = reduceCommand(state, operation, operation.seq);
  if (!result.ok) throw new Error(`${result.code}: ${result.message}`);
  return result.state;
}

export function optimisticState(committed: BoardState, pending: DurableCommand[]): BoardState {
  return pending.reduce((state, command) => {
    const result = reduceCommand(state, command, state.lastSeq + 1);
    return result.ok ? result.state : state;
  }, committed);
}

function canonicalValue(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(canonicalValue);
  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value)
        .filter(([, item]) => item !== undefined)
        .sort(([left], [right]) => left.localeCompare(right))
        .map(([key, item]) => [key, canonicalValue(item)]),
    );
  }
  if (typeof value === "number" && Object.is(value, -0)) return 0;
  return value;
}

export function canonicalBoard(state: BoardState): string {
  const active = Object.values(state.objects)
    .filter((object) => object.deletedSeq === null)
    .map((object) => object.value)
    .sort((left, right) => left.id.localeCompare(right.id));
  return JSON.stringify(canonicalValue({ objects: active }));
}

export async function hashBoardState(state: BoardState): Promise<string> {
  const bytes = new TextEncoder().encode(canonicalBoard(state));
  const digest = await crypto.subtle.digest("SHA-256", bytes);
  return Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, "0")).join("");
}

export function visibleObjects(state: BoardState): CanvasObject[] {
  return state.order.flatMap((id) => {
    const projected = state.objects[id];
    return projected?.deletedSeq === null ? [projected.value] : [];
  });
}
