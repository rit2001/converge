import {
  canvasObjectSchema,
  type CanvasObject,
  type CommittedOperation,
  type DurableCommand,
} from "@converge/protocol";

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
  | {
      ok: false;
      code: "INVALID_COMMAND" | "TARGET_NOT_FOUND" | "TARGET_DELETED" | "CONFLICT";
      message: string;
    };

export const emptyBoardState = (): BoardState => ({ lastSeq: 0, objects: {}, order: [] });

function activeObject(state: BoardState, targetId: string): ProjectedObject | undefined {
  const projected = state.objects[targetId];
  return projected?.deletedSeq === null ? projected : undefined;
}

function updateFields(
  projected: ProjectedObject,
  payload: object,
  seq: number,
): ProjectedObject | undefined {
  const candidate = { ...projected.value, ...payload };
  const parsed = canvasObjectSchema.safeParse(candidate);
  if (!parsed.success) return undefined;
  const fieldSeq = { ...projected.fieldSeq };
  for (const field of Object.keys(payload)) fieldSeq[field] = seq;
  return { ...projected, value: parsed.data, fieldSeq, updatedSeq: seq };
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
    const parsed = canvasObjectSchema.safeParse(command.payload);
    if (!parsed.success) {
      return {
        ok: false,
        code: "INVALID_COMMAND",
        message: "Object does not satisfy its kind invariant",
      };
    }
    const fieldSeq = Object.fromEntries(Object.keys(parsed.data).map((field) => [field, seq]));
    return {
      ok: true,
      state: {
        lastSeq: seq,
        objects: {
          ...state.objects,
          [command.targetId]: {
            value: parsed.data,
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
    const parsed = canvasObjectSchema.safeParse(projected.value);
    if (!parsed.success) {
      return {
        ok: false,
        code: "INVALID_COMMAND",
        message: "Object does not satisfy its kind invariant",
      };
    }
    return {
      ok: true,
      state: {
        lastSeq: seq,
        objects: {
          ...state.objects,
          [command.targetId]: {
            ...projected,
            value: parsed.data,
            deletedSeq: seq,
            updatedSeq: seq,
          },
        },
        order: state.order.filter((id) => id !== command.targetId),
      },
    };
  }

  const next = updateFields(projected, command.payload, seq);
  if (!next) {
    return {
      ok: false,
      code: "INVALID_COMMAND",
      message: `Patch is incompatible with ${projected.value.kind} object`,
    };
  }
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
