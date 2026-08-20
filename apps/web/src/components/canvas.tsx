"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Group, Layer, Line, Rect, Stage, Text, Transformer } from "react-konva";
import type Konva from "konva";
import type { CanvasObject } from "@converge/protocol";
import { normalizeRotation, rotationLabel, rotationPreviewAngle } from "../canvas/rotation";
import { CANVAS_GRID_SPACING, type AlignmentGuide, snapObjectPosition } from "../canvas/snapping";

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
  }, [clearGuides, hiddenObjectIds, lockedObjectIds, objects, tool]);

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

  return (
    <div
      className="canvas-shell"
      ref={container}
      style={
        {
          "--canvas-grid-size": `${CANVAS_GRID_SPACING * stage.scale}px`,
          "--canvas-grid-offset": `${stage.x}px ${stage.y}px`,
        } as React.CSSProperties
      }
    >
      <Stage
        ref={stageRef}
        width={viewport.width}
        height={viewport.height}
        x={stage.x}
        y={stage.y}
        scaleX={stage.scale}
        scaleY={stage.scale}
        draggable={tool === "pan"}
        onDragEnd={(event) => {
          clearGuides();
          if (event.target === stageRef.current)
            setStage((value) => ({ ...value, x: event.target.x(), y: event.target.y() }));
        }}
        onPointerCancel={() => {
          cancelledDragId.current = activeDragId.current;
          clearGuides();
          fenceRotationPreview();
        }}
        onPointerDown={(event) => {
          if (event.target === event.target.getStage()) onSelect(null);
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
                draggable={tool === "select" && !locked}
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
                  if (!locked && !wasCancelled)
                    onTransform(object.id, { x: event.target.x(), y: event.target.y() });
                }}
                onTransformEnd={(event) => {
                  if (locked) return;
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
      </Stage>
      <output className="zoom-pill" aria-label={`Canvas zoom: ${Math.round(stage.scale * 100)}%`}>
        {Math.round(stage.scale * 100)}%
      </output>
    </div>
  );
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
