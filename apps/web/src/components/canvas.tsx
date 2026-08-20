"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Group, Layer, Line, Rect, Stage, Text, Transformer } from "react-konva";
import type Konva from "konva";
import type { CanvasObject } from "@converge/protocol";
import { normalizeRotation, rotationLabel, rotationPreviewAngle } from "../canvas/rotation";
import { CANVAS_GRID_SPACING, type AlignmentGuide, snapObjectPosition } from "../canvas/snapping";
import { keyboardObjectPatch, viewportCenter } from "../canvas/keyboard-manipulation";
import type { PresenceSnapshot } from "../presence-store";
import type { EditorCapability } from "../editor-capability";

const EMPTY_OBJECT_IDS: ReadonlySet<string> = new Set();

interface RotationPreview {
  id: string;
  angle: number;
}

interface Props {
  objects: CanvasObject[];
  selectedId: string | null;
  hiddenObjectIds?: ReadonlySet<string>;
  lockedObjectIds?: ReadonlySet<string>;
  tool: "select" | "pan";
  onSelect: (id: string | null) => void;
  onTransform: (
    id: string,
    patch: { x?: number; y?: number; width?: number; height?: number; rotation?: number },
  ) => void;
  rotationEnabled?: boolean;
  rotationFence?: string;
  presence?: PresenceSnapshot | null;
  onPresencePointer?: (cursor: { x: number; y: number } | null) => void;
  viewportCommand?: { id: number; action: "in" | "out" | "reset" } | undefined;
  creationTool?: "rectangle" | "sticky" | null;
  onCreateFromCanvas?: (kind: "rectangle" | "sticky") => void;
  keyboardEditingEnabled?: boolean;
  canvasFocusRequest?: number;
  onViewportCenterChange?: (center: { x: number; y: number }) => void;
  onKeyboardRejected?: (message: string) => void;
  editingEnabled?: boolean;
  capability?: EditorCapability;
}

export function Canvas({
  objects,
  selectedId,
  hiddenObjectIds = EMPTY_OBJECT_IDS,
  lockedObjectIds = EMPTY_OBJECT_IDS,
  tool,
  onSelect,
  onTransform,
  rotationEnabled = false,
  rotationFence = "initial",
  presence = null,
  onPresencePointer,
  viewportCommand,
  creationTool = null,
  onCreateFromCanvas,
  keyboardEditingEnabled = false,
  canvasFocusRequest = 0,
  onViewportCenterChange,
  onKeyboardRejected,
  editingEnabled = true,
  capability = "full_edit",
}: Props): React.JSX.Element {
  const [viewport, setViewport] = useState({ width: 900, height: 600 });
  const [stage, setStage] = useState({ x: 0, y: 0, scale: 1 });
  const container = useRef<HTMLDivElement>(null);
  const stageRef = useRef<Konva.Stage>(null);
  const transformerRef = useRef<Konva.Transformer>(null);
  const activeDragId = useRef<string | null>(null);
  const cancelledDragId = useRef<string | null>(null);
  const rotationGesture = useRef<{ id: string; fence: string } | null>(null);
  const rotationInteraction = useRef<{ id: string; fence: string } | null>(null);
  const fencedRotationId = useRef<string | null>(null);
  const [guides, setGuides] = useState<AlignmentGuide[]>([]);
  const [guideReferenceIds, setGuideReferenceIds] = useState<string[]>([]);
  const [guideStroke, setGuideStroke] = useState("#1769aa");
  const [rotationFeedbackStroke, setRotationFeedbackStroke] = useState("#5146d8");
  const [rotationHandleFill, setRotationHandleFill] = useState("#ffffff");
  const [rotationPreview, setRotationPreview] = useState<RotationPreview | null>(null);
  const touchGesture = useRef<{
    distance: number;
    startScale: number;
    world: { x: number; y: number };
  } | null>(null);
  const ignoreMouseUntil = useRef(0);
  const visibleObjects = useMemo(
    () => objects.filter((object) => !hiddenObjectIds.has(object.id)),
    [hiddenObjectIds, objects],
  );
  const clearGuides = useCallback(() => {
    setGuides((current) => (current.length === 0 ? current : []));
    setGuideReferenceIds((current) => (current.length === 0 ? current : []));
  }, []);
  const visibleObjectIds = useMemo(
    () => new Set(visibleObjects.map((object) => object.id)),
    [visibleObjects],
  );
  const renderedGuides =
    (!activeDragId.current || visibleObjectIds.has(activeDragId.current)) &&
    guideReferenceIds.every((id) => visibleObjectIds.has(id))
      ? guides
      : [];
  const canRotate = Boolean(
    editingEnabled &&
      rotationEnabled &&
      tool === "select" &&
      selectedId &&
      visibleObjectIds.has(selectedId) &&
      !lockedObjectIds.has(selectedId),
  );
  const latestRotationState = useRef({
    selectedId,
    visibleObjectIds,
    lockedObjectIds,
    canRotate,
    rotationFence,
  });
  latestRotationState.current = {
    selectedId,
    visibleObjectIds,
    lockedObjectIds,
    canRotate,
    rotationFence,
  };
  const rotationPreviewObject =
    rotationPreview && rotationPreview.id === selectedId && canRotate
      ? visibleObjects.find((object) => object.id === rotationPreview.id)
      : undefined;

  useEffect(() => {
    const resize = (): void => {
      if (container.current)
        setViewport({
          width: container.current.clientWidth,
          height: container.current.clientHeight,
        });
    };
    resize();
    const observer = new ResizeObserver(resize);
    if (container.current) observer.observe(container.current);
    return () => observer.disconnect();
  }, []);
  useEffect(() => {
    if (!viewportCommand) return;
    setStage((value) => {
      const scale =
        viewportCommand.action === "reset"
          ? 1
          : Math.min(
              3,
              Math.max(0.25, value.scale * (viewportCommand.action === "in" ? 1.1 : 0.9)),
            );
      const center = { x: viewport.width / 2, y: viewport.height / 2 };
      const world = {
        x: (center.x - value.x) / value.scale,
        y: (center.y - value.y) / value.scale,
      };
      return { scale, x: center.x - world.x * scale, y: center.y - world.y * scale };
    });
  }, [viewportCommand, viewport.height, viewport.width]);
  useEffect(() => {
    onViewportCenterChange?.(viewportCenter(stage, viewport));
  }, [onViewportCenterChange, stage, viewport]);
  useEffect(() => {
    if (canvasFocusRequest <= 0) return;
    const frame = requestAnimationFrame(() => container.current?.focus());
    return () => cancelAnimationFrame(frame);
  }, [canvasFocusRequest]);
  useEffect(() => {
    const transformer = transformerRef.current;
    const node =
      selectedId && !lockedObjectIds.has(selectedId)
        ? stageRef.current?.findOne(`#object-${selectedId}`)
        : undefined;
    transformer?.nodes(node ? [node] : []);
    transformer?.getLayer()?.batchDraw();
  }, [selectedId, visibleObjects, lockedObjectIds]);

  useEffect(() => {
    clearGuides();
  }, [capability, clearGuides, hiddenObjectIds, lockedObjectIds, objects, tool]);

  const clearRotationPreview = useCallback(() => {
    rotationInteraction.current = null;
    setRotationPreview((current) => (current === null ? current : null));
  }, []);

  const fenceRotationPreview = useCallback(() => {
    fencedRotationId.current =
      rotationInteraction.current?.id ?? rotationGesture.current?.id ?? fencedRotationId.current;
    rotationGesture.current = null;
    clearRotationPreview();
  }, [clearRotationPreview]);

  useEffect(() => {
    if (capability !== "view_only") return;
    cancelledDragId.current = activeDragId.current;
    activeDragId.current = null;
    touchGesture.current = null;
    stageRef.current?.stopDrag();
    clearGuides();
    fenceRotationPreview();
    onPresencePointer?.(null);
  }, [capability, clearGuides, fenceRotationPreview, onPresencePointer]);

  useEffect(() => {
    const clearTouch = (): void => {
      touchGesture.current = null;
      cancelledDragId.current = activeDragId.current;
      activeDragId.current = null;
      clearGuides();
      fenceRotationPreview();
      onPresencePointer?.(null);
    };
    window.addEventListener("blur", clearTouch);
    return () => {
      window.removeEventListener("blur", clearTouch);
      clearTouch();
    };
  }, [clearGuides, fenceRotationPreview, onPresencePointer]);

  useEffect(() => {
    fenceRotationPreview();
  }, [
    canRotate,
    fenceRotationPreview,
    hiddenObjectIds,
    lockedObjectIds,
    objects,
    rotationFence,
    selectedId,
  ]);

  useEffect(() => {
    const resolveCanvasToken = (name: string, fallback: string): string => {
      if (!container.current) return fallback;
      const token = getComputedStyle(container.current).getPropertyValue(name).trim() || fallback;
      const probe = document.createElement("span");
      probe.style.color = token;
      container.current.append(probe);
      const resolved = getComputedStyle(probe).color;
      probe.remove();
      return resolved || fallback;
    };
    const updateCanvasStrokes = (): void => {
      if (!container.current) return;
      setGuideStroke(resolveCanvasToken("--color-alignment-guide", "#1769aa"));
      setRotationFeedbackStroke(resolveCanvasToken("--color-rotation-feedback", "#5146d8"));
      setRotationHandleFill(resolveCanvasToken("--color-rotation-handle-fill", "#ffffff"));
    };
    updateCanvasStrokes();
    const forcedColors = window.matchMedia("(forced-colors: active)");
    forcedColors.addEventListener("change", updateCanvasStrokes);
    return () => forcedColors.removeEventListener("change", updateCanvasStrokes);
  }, []);

  useEffect(
    () => () => {
      activeDragId.current = null;
      cancelledDragId.current = null;
      rotationGesture.current = null;
      rotationInteraction.current = null;
      fencedRotationId.current = null;
    },
    [],
  );

  useEffect(() => {
    const clear = (): void => onPresencePointer?.(null);
    const visibility = (): void => {
      if (document.hidden) clear();
    };
    window.addEventListener("blur", clear);
    document.addEventListener("visibilitychange", visibility);
    return () => {
      window.removeEventListener("blur", clear);
      document.removeEventListener("visibilitychange", visibility);
      clear();
    };
  }, [onPresencePointer]);

  return (
    <div
      className="canvas-shell"
      ref={container}
      tabIndex={0}
      aria-label="Canvas editing surface"
      aria-describedby="canvas-editing-instructions"
      onKeyDown={(event) => {
        if (
          event.repeat ||
          event.ctrlKey ||
          event.metaKey ||
          !keyboardEditingEnabled ||
          !selectedId ||
          hiddenObjectIds.has(selectedId) ||
          lockedObjectIds.has(selectedId) ||
          !(["ArrowLeft", "ArrowRight", "ArrowUp", "ArrowDown"] as const).includes(
            event.key as never,
          )
        )
          return;
        const object = objects.find((candidate) => candidate.id === selectedId);
        if (!object) return;
        const patch = keyboardObjectPatch(
          object,
          event.key as "ArrowLeft" | "ArrowRight" | "ArrowUp" | "ArrowDown",
          { altKey: event.altKey, shiftKey: event.shiftKey },
        );
        if (!patch) {
          onKeyboardRejected?.(
            event.altKey
              ? "Object size is already at its allowed limit."
              : "Object position is already at its allowed limit.",
          );
          return;
        }
        event.preventDefault();
        onTransform(selectedId, patch);
      }}
      style={
        {
          "--canvas-grid-size": `${CANVAS_GRID_SPACING * stage.scale}px`,
          "--canvas-grid-offset": `${stage.x}px ${stage.y}px`,
        } as React.CSSProperties
      }
    >
      <span id="canvas-editing-instructions" className="ui-visually-hidden">
        Focus the canvas to move a selected object with arrow keys. Hold Shift for ten units or
        Alt/Option to resize.
      </span>
      <Stage
        ref={stageRef}
        width={viewport.width}
        height={viewport.height}
        x={stage.x}
        y={stage.y}
        scaleX={stage.scale}
        scaleY={stage.scale}
        draggable={tool === "pan" || capability === "view_only"}
        onTouchStart={(event) => {
          const touches = event.evt.touches;
          if (touches.length < 2) return;
          ignoreMouseUntil.current = Date.now() + 800;
          cancelledDragId.current = activeDragId.current;
          activeDragId.current = null;
          stageRef.current?.stopDrag();
          clearGuides();
          fenceRotationPreview();
          const [first, second] = [touches[0], touches[1]];
          if (!first || !second) return;
          const midpoint = {
            x: (first.clientX + second.clientX) / 2,
            y: (first.clientY + second.clientY) / 2,
          };
          touchGesture.current = {
            distance: Math.hypot(second.clientX - first.clientX, second.clientY - first.clientY),
            startScale: stage.scale,
            world: {
              x: (midpoint.x - stage.x) / stage.scale,
              y: (midpoint.y - stage.y) / stage.scale,
            },
          };
          onPresencePointer?.(null);
        }}
        onTouchMove={(event) => {
          const gesture = touchGesture.current;
          const touches = event.evt.touches;
          if (!gesture || touches.length < 2) return;
          event.evt.preventDefault();
          const [first, second] = [touches[0], touches[1]];
          if (!first || !second) return;
          const midpoint = {
            x: (first.clientX + second.clientX) / 2,
            y: (first.clientY + second.clientY) / 2,
          };
          const distance = Math.hypot(
            second.clientX - first.clientX,
            second.clientY - first.clientY,
          );
          if (!Number.isFinite(distance) || gesture.distance <= 0) return;
          const scale = Math.min(
            3,
            Math.max(0.25, gesture.startScale * (distance / gesture.distance)),
          );
          setStage({
            scale,
            x: midpoint.x - gesture.world.x * scale,
            y: midpoint.y - gesture.world.y * scale,
          });
        }}
        onTouchEnd={(event) => {
          if (event.evt.touches.length < 2) touchGesture.current = null;
          onPresencePointer?.(null);
        }}
        onTouchCancel={() => {
          touchGesture.current = null;
          cancelledDragId.current = activeDragId.current;
          activeDragId.current = null;
          clearGuides();
          fenceRotationPreview();
          onPresencePointer?.(null);
        }}
        onDragEnd={(event) => {
          clearGuides();
          if (event.target === stageRef.current)
            setStage((value) => ({ ...value, x: event.target.x(), y: event.target.y() }));
        }}
        onPointerCancel={() => {
          cancelledDragId.current = activeDragId.current;
          clearGuides();
          fenceRotationPreview();
          onPresencePointer?.(null);
        }}
        onPointerMove={(event) => {
          const pointerType = event.evt.pointerType;
          if (pointerType !== "mouse" && pointerType !== "pen") return;
          const point = stageRef.current?.getPointerPosition();
          if (!point) return;
          onPresencePointer?.({
            x: (point.x - stage.x) / stage.scale,
            y: (point.y - stage.y) / stage.scale,
          });
        }}
        onPointerLeave={() => onPresencePointer?.(null)}
        onPointerDown={(event) => {
          if (event.evt?.pointerType === "mouse" && Date.now() < ignoreMouseUntil.current) return;
          if (event.target !== event.target.getStage()) return;
          if (creationTool && editingEnabled) {
            onCreateFromCanvas?.(creationTool);
            return;
          }
          onSelect(null);
        }}
        onWheel={(event) => {
          event.evt.preventDefault();
          const current = stageRef.current;
          const pointer = current?.getPointerPosition();
          if (!current || !pointer) return;
          const nextScale = Math.min(
            3,
            Math.max(0.25, stage.scale * (event.evt.deltaY > 0 ? 0.9 : 1.1)),
          );
          const point = {
            x: (pointer.x - stage.x) / stage.scale,
            y: (pointer.y - stage.y) / stage.scale,
          };
          setStage({
            scale: nextScale,
            x: pointer.x - point.x * nextScale,
            y: pointer.y - point.y * nextScale,
          });
        }}
      >
        <Layer>
          {visibleObjects.map((object) => {
            const locked = lockedObjectIds.has(object.id);
            return (
              <Group
                key={object.id}
                id={`object-${object.id}`}
                x={object.x}
                y={object.y}
                width={object.width}
                height={object.height}
                rotation={object.rotation}
                draggable={editingEnabled && tool === "select" && !locked}
                onClick={() => {
                  if (!locked) onSelect(object.id);
                }}
                onTap={() => {
                  if (!locked) onSelect(object.id);
                }}
                onDragStart={() => {
                  activeDragId.current = object.id;
                  cancelledDragId.current = null;
                  clearGuides();
                }}
                onDragMove={(event) => {
                  if (locked || event.evt.altKey) {
                    clearGuides();
                    return;
                  }
                  const snapped = snapObjectPosition({
                    object,
                    position: { x: event.target.x(), y: event.target.y() },
                    objects,
                    hiddenObjectIds,
                    viewportScale: stage.scale,
                  });
                  event.target.position(snapped.position);
                  setGuides(snapped.guides);
                  setGuideReferenceIds(snapped.referenceIds);
                }}
                onDragEnd={(event) => {
                  const wasCancelled = cancelledDragId.current === object.id;
                  clearGuides();
                  activeDragId.current = null;
                  cancelledDragId.current = null;
                  if (editingEnabled && !locked && !wasCancelled)
                    onTransform(object.id, { x: event.target.x(), y: event.target.y() });
                }}
                onTransformEnd={(event) => {
                  if (locked || !editingEnabled) return;
                  const node = event.target;
                  if (fencedRotationId.current === object.id) {
                    fencedRotationId.current = null;
                    return;
                  }
                  const gesture = rotationGesture.current;
                  if (gesture?.id === object.id) {
                    rotationGesture.current = null;
                    const interaction = rotationInteraction.current;
                    const latest = latestRotationState.current;
                    const canCommitRotation =
                      interaction?.fence === latest.rotationFence &&
                      latest.canRotate &&
                      latest.selectedId === object.id &&
                      latest.visibleObjectIds.has(object.id) &&
                      !latest.lockedObjectIds.has(object.id);
                    const rotation = normalizeRotation(node.rotation());
                    node.rotation(rotation);
                    clearRotationPreview();
                    if (interaction && canCommitRotation) onTransform(object.id, { rotation });
                    return;
                  }
                  const interaction = rotationInteraction.current;
                  const latest = latestRotationState.current;
                  if (interaction?.id === object.id) {
                    const canCommitRotation =
                      interaction.fence === latest.rotationFence &&
                      latest.canRotate &&
                      latest.selectedId === object.id &&
                      latest.visibleObjectIds.has(object.id) &&
                      !latest.lockedObjectIds.has(object.id);
                    const rotation = normalizeRotation(node.rotation());
                    node.rotation(rotation);
                    clearRotationPreview();
                    fencedRotationId.current = null;
                    if (canCommitRotation) onTransform(object.id, { rotation });
                    return;
                  }
                  const width = Math.max(8, object.width * node.scaleX());
                  const height = Math.max(8, object.height * node.scaleY());
                  node.scaleX(1);
                  node.scaleY(1);
                  onTransform(object.id, { x: node.x(), y: node.y(), width, height });
                }}
                onTransform={(event) => {
                  if (!canRotate || selectedId !== object.id || locked) return;
                  const node = event.target;
                  const shiftKey = (event.evt as MouseEvent).shiftKey;
                  const angle = rotationPreviewAngle(node.rotation(), shiftKey);
                  if (angle === normalizeRotation(object.rotation)) return;
                  node.rotation(angle);
                  fencedRotationId.current = null;
                  rotationInteraction.current = { id: object.id, fence: rotationFence };
                  setRotationPreview({ id: object.id, angle });
                }}
                onTransformStart={() => {
                  if (
                    canRotate &&
                    selectedId === object.id &&
                    !locked &&
                    transformerRef.current?.getActiveAnchor() === "rotater"
                  ) {
                    rotationGesture.current = { id: object.id, fence: rotationFence };
                  }
                }}
              >
                <Rect
                  width={object.width}
                  height={object.height}
                  fill={object.fill}
                  cornerRadius={object.kind === "sticky" ? 4 : 12}
                  shadowColor="#0f172a"
                  shadowBlur={selectedId === object.id ? 16 : 8}
                  shadowOpacity={0.16}
                  shadowOffsetY={4}
                />
                {object.kind === "sticky" && (
                  <Text
                    text={object.text}
                    width={object.width}
                    height={object.height}
                    padding={18}
                    fontSize={18}
                    fontFamily="Inter, sans-serif"
                    fill="#312e1f"
                    wrap="word"
                  />
                )}
              </Group>
            );
          })}
        </Layer>
        <Layer name="canvas-controls">
          <AlignmentGuides guides={renderedGuides} stroke={guideStroke} />
          {rotationPreviewObject && rotationPreview && (
            <Text
              text={rotationLabel(rotationPreview.angle)}
              x={rotationPreviewObject.x + rotationPreviewObject.width / 2}
              y={rotationPreviewObject.y - 30 / stage.scale}
              offsetX={18 / stage.scale}
              fontSize={12 / stage.scale}
              fontStyle="bold"
              fill={rotationFeedbackStroke}
              listening={false}
              perfectDrawEnabled={false}
            />
          )}
          <Transformer
            ref={transformerRef}
            rotateEnabled={canRotate}
            flipEnabled={false}
            borderStroke={rotationFeedbackStroke}
            anchorStroke={rotationFeedbackStroke}
            anchorFill={rotationHandleFill}
            anchorSize={10 / stage.scale}
            rotateAnchorOffset={20 / stage.scale}
            rotateAnchorCursor="crosshair"
            boundBoxFunc={(oldBox, newBox) =>
              newBox.width < 24 || newBox.height < 24 ? oldBox : newBox
            }
          />
        </Layer>
        <PresenceLayer presence={presence} stage={stage} viewport={viewport} />
      </Stage>
      <output className="zoom-pill" aria-label={`Canvas zoom: ${Math.round(stage.scale * 100)}%`}>
        {Math.round(stage.scale * 100)}%
      </output>
    </div>
  );
}

function PresenceLayer({
  presence,
  stage,
  viewport,
}: {
  presence: PresenceSnapshot | null;
  stage: { x: number; y: number; scale: number };
  viewport: { width: number; height: number };
}): React.JSX.Element {
  const reducedMotion = useReducedMotion();
  const [colors, setColors] = useState<Record<string, string>>({});
  useEffect(() => {
    const update = (): void => {
      const styles = getComputedStyle(document.documentElement);
      setColors(
        Object.fromEntries(
          Array.from({ length: 8 }, (_, index) => {
            const token = `collaborator-${index + 1}`;
            return [token, styles.getPropertyValue(`--color-${token}`).trim() || "CanvasText"];
          }),
        ),
      );
    };
    update();
    const media = window.matchMedia("(forced-colors: active)");
    media.addEventListener("change", update);
    return () => media.removeEventListener("change", update);
  }, []);
  const targets = useMemo(() => {
    if (!presence || !presence.current || presence.availability !== "available") return [];
    const margin = 96 / stage.scale;
    const left = -stage.x / stage.scale - margin;
    const top = -stage.y / stage.scale - margin;
    const right = left + viewport.width / stage.scale + margin * 2;
    const bottom = top + viewport.height / stage.scale + margin * 2;
    return presence.collaborators.filter(
      (person) =>
        !person.self &&
        person.activity === "active" &&
        person.cursor !== null &&
        person.cursor.x >= left &&
        person.cursor.x <= right &&
        person.cursor.y >= top &&
        person.cursor.y <= bottom,
    );
  }, [presence, stage, viewport]);
  const targetRef = useRef(targets);
  const displayed = useRef(new Map<string, { x: number; y: number }>());
  const [points, setPoints] = useState(() => new Map<string, { x: number; y: number }>());
  useEffect(() => {
    targetRef.current = targets;
  }, [targets]);
  useEffect(() => {
    let frame = 0;
    let cancelled = false;
    const targetIds = new Set(targets.map((item) => item.key));
    for (const id of displayed.current.keys()) if (!targetIds.has(id)) displayed.current.delete(id);
    const tick = (): void => {
      if (cancelled) return;
      let moving = false;
      const next = new Map<string, { x: number; y: number }>();
      for (const item of targetRef.current) {
        const target = item.cursor!;
        const previous = displayed.current.get(item.key) ?? target;
        const jump = Math.hypot(target.x - previous.x, target.y - previous.y) > 800;
        const fraction = reducedMotion || jump ? 1 : 0.55;
        const point = {
          x: previous.x + (target.x - previous.x) * fraction,
          y: previous.y + (target.y - previous.y) * fraction,
        };
        if (Math.abs(target.x - point.x) > 0.1 || Math.abs(target.y - point.y) > 0.1) moving = true;
        displayed.current.set(item.key, point);
        next.set(item.key, point);
      }
      setPoints(next);
      if (moving) frame = requestAnimationFrame(tick);
    };
    tick();
    return () => {
      cancelled = true;
      if (frame) cancelAnimationFrame(frame);
    };
  }, [reducedMotion, targets]);
  return (
    <Layer name="canvas-presence" listening={false}>
      {targets.map((person) => {
        const point = points.get(person.key) ?? person.cursor!;
        const color = colors[person.paletteToken] ?? "CanvasText";
        const name =
          person.displayName.length > 24
            ? `${person.displayName.slice(0, 23)}…`
            : person.displayName;
        return (
          <Group key={person.key} x={point.x} y={point.y} listening={false}>
            <Line
              points={[0, 0, 0, 18, 5, 13, 9, 21, 12, 19, 8, 11, 15, 11]}
              closed
              fill={color}
              stroke={color}
              listening={false}
            />
            <Text
              text={name}
              x={14}
              y={14}
              padding={4}
              fontSize={12 / stage.scale}
              fill={color}
              listening={false}
            />
          </Group>
        );
      })}
    </Layer>
  );
}

function useReducedMotion(): boolean {
  const [reduced, setReduced] = useState(false);
  useEffect(() => {
    const media = window.matchMedia("(prefers-reduced-motion: reduce)");
    const update = (): void => setReduced(media.matches);
    update();
    media.addEventListener("change", update);
    return () => media.removeEventListener("change", update);
  }, []);
  return reduced;
}

export function AlignmentGuides({
  guides,
  stroke = "#1769aa",
}: {
  guides: readonly AlignmentGuide[];
  stroke?: string;
}): React.JSX.Element {
  return (
    <>
      {guides.map((guide) => (
        <Line
          key={`${guide.axis}-${guide.coordinate}-${guide.from}-${guide.to}`}
          points={
            guide.axis === "vertical"
              ? [guide.coordinate, guide.from, guide.coordinate, guide.to]
              : [guide.from, guide.coordinate, guide.to, guide.coordinate]
          }
          stroke={stroke}
          strokeWidth={1}
          dash={[4, 4]}
          listening={false}
          perfectDrawEnabled={false}
        />
      ))}
    </>
  );
}
