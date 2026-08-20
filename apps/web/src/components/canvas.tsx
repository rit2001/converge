"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Group, Layer, Line, Rect, Stage, Text, Transformer } from "react-konva";
import type Konva from "konva";
import type { CanvasObject } from "@converge/protocol";
import { CANVAS_GRID_SPACING, type AlignmentGuide, snapObjectPosition } from "../canvas/snapping";

const EMPTY_OBJECT_IDS: ReadonlySet<string> = new Set();

interface Props {
  objects: CanvasObject[];
  selectedId: string | null;
  hiddenObjectIds?: ReadonlySet<string>;
  lockedObjectIds?: ReadonlySet<string>;
  tool: "select" | "pan";
  onSelect: (id: string | null) => void;
  onTransform: (
    id: string,
    patch: { x: number; y: number; width?: number; height?: number },
  ) => void;
}

export function Canvas({
  objects,
  selectedId,
  hiddenObjectIds = EMPTY_OBJECT_IDS,
  lockedObjectIds = EMPTY_OBJECT_IDS,
  tool,
  onSelect,
  onTransform,
}: Props): React.JSX.Element {
  const [viewport, setViewport] = useState({ width: 900, height: 600 });
  const [stage, setStage] = useState({ x: 0, y: 0, scale: 1 });
  const container = useRef<HTMLDivElement>(null);
  const stageRef = useRef<Konva.Stage>(null);
  const transformerRef = useRef<Konva.Transformer>(null);
  const activeDragId = useRef<string | null>(null);
  const cancelledDragId = useRef<string | null>(null);
  const [guides, setGuides] = useState<AlignmentGuide[]>([]);
  const [guideReferenceIds, setGuideReferenceIds] = useState<string[]>([]);
  const [guideStroke, setGuideStroke] = useState("#1769aa");
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

  useEffect(() => {
    const updateGuideStroke = (): void => {
      if (!container.current) return;
      const token =
        getComputedStyle(container.current).getPropertyValue("--color-alignment-guide").trim() ||
        "#1769aa";
      const probe = document.createElement("span");
      probe.style.color = token;
      container.current.append(probe);
      const resolved = getComputedStyle(probe).color;
      probe.remove();
      setGuideStroke(resolved || "#1769aa");
    };
    updateGuideStroke();
    const forcedColors = window.matchMedia("(forced-colors: active)");
    forcedColors.addEventListener("change", updateGuideStroke);
    return () => forcedColors.removeEventListener("change", updateGuideStroke);
  }, []);

  useEffect(
    () => () => {
      activeDragId.current = null;
      cancelledDragId.current = null;
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
                  const width = Math.max(8, object.width * node.scaleX());
                  const height = Math.max(8, object.height * node.scaleY());
                  node.scaleX(1);
                  node.scaleY(1);
                  onTransform(object.id, { x: node.x(), y: node.y(), width, height });
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
          <Transformer
            ref={transformerRef}
            rotateEnabled={false}
            flipEnabled={false}
            borderStroke="#4f46e5"
            anchorStroke="#4f46e5"
            anchorFill="#ffffff"
            anchorSize={10}
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
