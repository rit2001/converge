import type { CanvasObject } from "@converge/protocol";

/**
 * The board grid is an infinite 22-by-22 world-unit lattice anchored at (0, 0).
 * Canvas presentation scales and pans this lattice with the viewport; snapping uses
 * the same world geometry and only aligns an object's leading edge to the lattice.
 */
export const CANVAS_GRID_SPACING = 22;

/**
 * Snapping is allowed when its adjustment is within eight CSS (screen) pixels.
 * Convert this to world units with `threshold / viewportScale` before comparing it
 * to board coordinates so its physical feel is unchanged at every zoom level.
 */
export const SNAP_THRESHOLD_SCREEN_PX = 8;

export interface AlignmentGuide {
  axis: "vertical" | "horizontal";
  coordinate: number;
  from: number;
  to: number;
}

export interface SnapPositionInput {
  object: CanvasObject;
  position: { x: number; y: number };
  objects: readonly CanvasObject[];
  hiddenObjectIds?: ReadonlySet<string>;
  viewportScale: number;
  gridSpacing?: number | null;
  thresholdScreenPx?: number;
}

export interface SnapPositionResult {
  position: { x: number; y: number };
  guides: AlignmentGuide[];
  /** Internal lifecycle dependency ids; never render these in product UI or accessibility text. */
  referenceIds: string[];
}

interface AxisMatch {
  adjustment: number;
  source: "object" | "grid";
  reference?: CanvasObject;
  referenceAnchor: number;
  movingAnchor: number;
}

interface AxisCandidate extends AxisMatch {
  sourceId: string;
  sourceRank: number;
  referenceAnchorRank: number;
  movingAnchorRank: number;
}

function axisAnchors(origin: number, size: number): readonly number[] {
  return [origin, origin + size / 2, origin + size];
}

function nearestGridCoordinate(value: number, spacing: number): number {
  const lower = Math.floor(value / spacing) * spacing;
  const upper = lower + spacing;
  return value - lower <= upper - value ? lower : upper;
}

function compareCandidates(left: AxisCandidate, right: AxisCandidate, scale: number): number {
  const adjustment = Math.abs(left.adjustment) * scale - Math.abs(right.adjustment) * scale;
  if (adjustment !== 0) return adjustment;
  if (left.sourceRank !== right.sourceRank) return left.sourceRank - right.sourceRank;
  const source = left.sourceId.localeCompare(right.sourceId);
  if (source !== 0) return source;
  if (left.referenceAnchorRank !== right.referenceAnchorRank)
    return left.referenceAnchorRank - right.referenceAnchorRank;
  if (left.movingAnchorRank !== right.movingAnchorRank)
    return left.movingAnchorRank - right.movingAnchorRank;
  return left.adjustment - right.adjustment;
}

function chooseAxisMatch({
  object,
  origin,
  size,
  objects,
  hiddenObjectIds,
  viewportScale,
  gridSpacing,
  thresholdScreenPx,
  axis,
}: {
  object: CanvasObject;
  origin: number;
  size: number;
  objects: readonly CanvasObject[];
  hiddenObjectIds: ReadonlySet<string>;
  viewportScale: number;
  gridSpacing: number | null;
  thresholdScreenPx: number;
  axis: "x" | "y";
}): AxisMatch | null {
  const thresholdWorld = thresholdScreenPx / viewportScale;
  const movingAnchors = axisAnchors(origin, size);
  const candidates: AxisCandidate[] = [];
  const visibleReferences = objects
    .filter((candidate) => candidate.id !== object.id && !hiddenObjectIds.has(candidate.id))
    .sort((left, right) => left.id.localeCompare(right.id));

  for (const reference of visibleReferences) {
    const referenceOrigin = axis === "x" ? reference.x : reference.y;
    const referenceSize = axis === "x" ? reference.width : reference.height;
    const referenceAnchors = axisAnchors(referenceOrigin, referenceSize);
    for (let movingAnchorRank = 0; movingAnchorRank < movingAnchors.length; movingAnchorRank += 1) {
      for (
        let referenceAnchorRank = 0;
        referenceAnchorRank < referenceAnchors.length;
        referenceAnchorRank += 1
      ) {
        const adjustment =
          referenceAnchors[referenceAnchorRank]! - movingAnchors[movingAnchorRank]!;
        if (Math.abs(adjustment) > thresholdWorld) continue;
        candidates.push({
          adjustment,
          source: "object",
          reference,
          sourceId: reference.id,
          sourceRank: 0,
          referenceAnchor: referenceAnchors[referenceAnchorRank]!,
          movingAnchor: movingAnchors[movingAnchorRank]!,
          referenceAnchorRank,
          movingAnchorRank,
        });
      }
    }
  }

  if (gridSpacing && gridSpacing > 0) {
    const gridCoordinate = nearestGridCoordinate(origin, gridSpacing);
    const adjustment = gridCoordinate - origin;
    if (Math.abs(adjustment) <= thresholdWorld) {
      candidates.push({
        adjustment,
        source: "grid",
        sourceId: "grid",
        sourceRank: 1,
        referenceAnchor: gridCoordinate,
        movingAnchor: origin,
        referenceAnchorRank: 0,
        movingAnchorRank: 0,
      });
    }
  }

  const winner = candidates.sort((left, right) => compareCandidates(left, right, viewportScale))[0];
  return winner ?? null;
}

function guideForMatch(
  match: AxisMatch | null,
  object: CanvasObject,
  position: { x: number; y: number },
  axis: "x" | "y",
): AlignmentGuide | null {
  if (!match?.reference) return null;
  if (axis === "x") {
    return {
      axis: "vertical",
      coordinate: match.referenceAnchor,
      from: Math.min(position.y, match.reference.y),
      to: Math.max(position.y + object.height, match.reference.y + match.reference.height),
    };
  }
  return {
    axis: "horizontal",
    coordinate: match.referenceAnchor,
    from: Math.min(position.x, match.reference.x),
    to: Math.max(position.x + object.width, match.reference.x + match.reference.width),
  };
}

/**
 * Selects each axis independently. Object-reference candidates are sorted by
 * absolute screen-space adjustment, then object before grid, lexical object id,
 * reference anchor (leading/center/trailing), moving anchor, and signed adjustment.
 * IDs remain implementation-only and are never rendered in product UI.
 */
export function snapObjectPosition({
  object,
  position,
  objects,
  hiddenObjectIds = new Set(),
  viewportScale,
  gridSpacing = CANVAS_GRID_SPACING,
  thresholdScreenPx = SNAP_THRESHOLD_SCREEN_PX,
}: SnapPositionInput): SnapPositionResult {
  if (!Number.isFinite(viewportScale) || viewportScale <= 0)
    return { position: { ...position }, guides: [], referenceIds: [] };

  const xMatch = chooseAxisMatch({
    object,
    origin: position.x,
    size: object.width,
    objects,
    hiddenObjectIds,
    viewportScale,
    gridSpacing,
    thresholdScreenPx,
    axis: "x",
  });
  const yMatch = chooseAxisMatch({
    object,
    origin: position.y,
    size: object.height,
    objects,
    hiddenObjectIds,
    viewportScale,
    gridSpacing,
    thresholdScreenPx,
    axis: "y",
  });
  const snappedPosition = {
    x: position.x + (xMatch?.adjustment ?? 0),
    y: position.y + (yMatch?.adjustment ?? 0),
  };
  return {
    position: snappedPosition,
    guides: [
      guideForMatch(xMatch, object, snappedPosition, "x"),
      guideForMatch(yMatch, object, snappedPosition, "y"),
    ].filter((guide): guide is AlignmentGuide => guide !== null),
    referenceIds: [xMatch?.reference?.id, yMatch?.reference?.id].filter((id): id is string =>
      Boolean(id),
    ),
  };
}
