import { useSyncExternalStore } from "react";

export type EditorCapability = "full_edit" | "compact_edit" | "view_only";
export const VIEW_ONLY_WIDTH = 640;

export function deriveEditorCapability(input: {
  width: number;
  height: number;
  coarsePointer: boolean;
}): EditorCapability {
  if (input.width < VIEW_ONLY_WIDTH) return "view_only";
  if (input.coarsePointer && input.height < 600 && input.width < 900) return "view_only";
  return input.width < 1000 ? "compact_edit" : "full_edit";
}

function subscribe(listener: () => void): () => void {
  const queries = [window.matchMedia("(pointer: coarse)"), window.matchMedia("(max-width: 999px)")];
  window.addEventListener("resize", listener);
  queries.forEach((query) => query.addEventListener("change", listener));
  return () => {
    window.removeEventListener("resize", listener);
    queries.forEach((query) => query.removeEventListener("change", listener));
  };
}

function snapshot(): EditorCapability {
  return deriveEditorCapability({
    width: window.innerWidth,
    height: window.innerHeight,
    coarsePointer: window.matchMedia("(pointer: coarse)").matches,
  });
}

export function useEditorCapability(): EditorCapability {
  return useSyncExternalStore(subscribe, snapshot, () => "full_edit");
}
