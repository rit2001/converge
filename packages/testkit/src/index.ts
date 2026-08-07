import type { DurableCommand } from "@converge/protocol";

export const fixtureIds = {
  user: "00000000-0000-4000-8000-000000000001",
  clientA: "10000000-0000-4000-8000-000000000001",
  clientB: "10000000-0000-4000-8000-000000000002",
};

export function createRectangleCommand(
  boardId: string,
  objectId = crypto.randomUUID(),
  opId = crypto.randomUUID(),
): DurableCommand {
  return {
    schemaVersion: 1,
    opId,
    boardId,
    clientId: fixtureIds.clientA,
    baseSeq: 0,
    type: "object.create",
    targetId: objectId,
    payload: {
      id: objectId,
      kind: "rectangle",
      x: 40,
      y: 40,
      width: 160,
      height: 100,
      rotation: 0,
      fill: "#818cf8",
      text: "",
    },
    clientTimestamp: new Date().toISOString(),
  };
}
