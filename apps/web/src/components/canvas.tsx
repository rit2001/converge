"use client";

import { useEffect, useRef, useState } from "react";
import { Group, Layer, Rect, Stage, Text, Transformer } from "react-konva";
import type Konva from "konva";
import type { CanvasObject } from "@converge/protocol";

interface Props {
  objects: CanvasObject[];
  selectedId: string | null;
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
  tool,
  onSelect,
  onTransform,
}: Props): React.JSX.Element {
  const [viewport, setViewport] = useState({ width: 900, height: 600 });
  const [stage, setStage] = useState({ x: 0, y: 0, scale: 1 });
  const container = useRef<HTMLDivElement>(null);
  const stageRef = useRef<Konva.Stage>(null);
  const transformerRef = useRef<Konva.Transformer>(null);

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
    const node = selectedId ? stageRef.current?.findOne(`#object-${selectedId}`) : undefined;
    transformer?.nodes(node ? [node] : []);
    transformer?.getLayer()?.batchDraw();
  }, [selectedId, objects]);

  return (
    <div className="canvas-shell" ref={container}>
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
          if (event.target === stageRef.current)
            setStage((value) => ({ ...value, x: event.target.x(), y: event.target.y() }));
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
          {objects.map((object) => (
            <Group
              key={object.id}
              id={`object-${object.id}`}
              x={object.x}
              y={object.y}
              width={object.width}
              height={object.height}
              rotation={object.rotation}
              draggable={tool === "select"}
              onClick={() => onSelect(object.id)}
              onTap={() => onSelect(object.id)}
              onDragEnd={(event) =>
                onTransform(object.id, { x: event.target.x(), y: event.target.y() })
              }
              onTransformEnd={(event) => {
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
          ))}
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
