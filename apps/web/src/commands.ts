export const COMMAND_IDS = [
  "tool.select",
  "tool.pan",
  "tool.rectangle",
  "tool.sticky",
  "view.zoom-in",
  "view.zoom-out",
  "view.reset-zoom",
  "view.focus-canvas",
  "create.rectangle-center",
  "create.sticky-center",
  "panel.layers",
  "panel.synchronization",
  "panel.collaborators",
  "panel.diagnostics",
  "panel.help",
  "panel.share",
  "theme.system",
  "theme.light",
  "theme.dark",
  "selection.delete",
  "selection.hide",
  "selection.lock",
  "selection.rotate-clockwise",
  "selection.rotate-counterclockwise",
  "selection.reset-rotation",
] as const;
export type CommandId = (typeof COMMAND_IDS)[number];
export type CommandCategory = "Tools" | "View" | "Panels" | "Selection" | "Create";
export type WorkspaceCommand = Readonly<{
  id: CommandId;
  label: string;
  description: string;
  category: CommandCategory;
  keywords: readonly string[];
  shortcut?: string | undefined;
  available: boolean;
  disabledReason?: string;
  execute(): void;
}>;

export const MAX_COMMAND_QUERY_LENGTH = 80;
export const MAX_COMMAND_RESULTS = 24;

export function searchCommands(
  commands: readonly WorkspaceCommand[],
  query: string,
): WorkspaceCommand[] {
  const normalized = query.trim().toLocaleLowerCase().slice(0, MAX_COMMAND_QUERY_LENGTH);
  if (!normalized) return commands.slice(0, MAX_COMMAND_RESULTS);
  return commands
    .map((command, index) => {
      const label = command.label.toLocaleLowerCase();
      const keywords = command.keywords.map((keyword) => keyword.toLocaleLowerCase());
      const remainder = `${command.description} ${command.category}`.toLocaleLowerCase();
      const rank =
        label === normalized
          ? 0
          : label.startsWith(normalized)
            ? 1
            : keywords.some((keyword) => keyword.startsWith(normalized))
              ? 2
              : label.includes(normalized)
                ? 3
                : remainder.includes(normalized)
                  ? 4
                  : 5;
      return { command, index, rank };
    })
    .filter((item) => item.rank < 5)
    .sort((left, right) => left.rank - right.rank || left.index - right.index)
    .slice(0, MAX_COMMAND_RESULTS)
    .map((item) => item.command);
}

type CommandInput = Readonly<{
  ready: boolean;
  hasSelection: boolean;
  selectedLocked: boolean;
  selectedHidden: boolean;
  rotateAvailable: boolean;
  canvasAvailable: boolean;
  mutationAllowed: boolean;
  themePreference?: "system" | "light" | "dark";
  viewOnly?: boolean;
  action: Record<CommandId, () => void>;
}>;
const unavailable = (enabled: boolean, reason: string) =>
  enabled ? {} : { disabledReason: reason };

export function createWorkspaceCommands(input: CommandInput): WorkspaceCommand[] {
  const add = (
    id: CommandId,
    label: string,
    description: string,
    category: CommandCategory,
    keywords: readonly string[],
    enabled = true,
    shortcut?: string,
  ): WorkspaceCommand => ({
    id,
    label,
    description,
    category,
    keywords,
    shortcut,
    available: enabled,
    ...unavailable(enabled, "This action is not available right now."),
    execute: input.action[id],
  });
  const selected =
    input.mutationAllowed && input.hasSelection && !input.selectedLocked && !input.selectedHidden;
  const themePreference = input.themePreference ?? "system";
  const selectionReason = !input.ready
    ? "Board is not ready for editing."
    : input.hasSelection
      ? input.selectedLocked
        ? "This object is locked in this view."
        : "This object is hidden in this view."
      : "Select an object first.";
  const commands = [
    add(
      "tool.select",
      "Select tool",
      "Select and move canvas objects",
      "Tools",
      ["select", "pointer"],
      true,
      "V",
    ),
    add("tool.pan", "Hand tool", "Pan the board viewport", "Tools", ["pan", "hand"], true, "H"),
    add(
      "tool.rectangle",
      "Rectangle tool",
      "Add a rectangle with the existing tool action",
      "Tools",
      ["rectangle", "shape"],
      input.mutationAllowed,
      "R",
    ),
    add(
      "tool.sticky",
      "Sticky-note tool",
      "Add a sticky note with the existing tool action",
      "Tools",
      ["sticky", "note"],
      input.mutationAllowed,
      "N",
    ),
    add("view.zoom-in", "Zoom in", "Increase canvas zoom", "View", ["zoom", "view"]),
    add("view.zoom-out", "Zoom out", "Decrease canvas zoom", "View", ["zoom", "view"]),
    add(
      "view.reset-zoom",
      "Reset zoom",
      "Return canvas zoom to 100%",
      "View",
      ["zoom", "reset"],
      true,
      "0",
    ),
    add(
      "view.focus-canvas",
      "Focus canvas",
      input.hasSelection
        ? "Focus canvas to edit the selected object"
        : "Focus the canvas editing surface",
      "View",
      ["canvas", "focus", "keyboard"],
      input.canvasAvailable,
    ),
    add(
      "create.rectangle-center",
      "Create rectangle at viewport center",
      "Create a rectangle at the visible board center",
      "Create",
      ["rectangle", "create", "center", "keyboard"],
      input.mutationAllowed,
    ),
    add(
      "create.sticky-center",
      "Create sticky note at viewport center",
      "Create a sticky note at the visible board center",
      "Create",
      ["sticky", "note", "create", "center", "keyboard"],
      input.mutationAllowed,
    ),
    add("panel.layers", "Open Layers", "Show board layers", "Panels", ["layers", "objects"]),
    add(
      "panel.synchronization",
      "Open synchronization details",
      "Show local synchronization evidence",
      "Panels",
      ["sync", "saving"],
    ),
    add("panel.collaborators", "Open collaborators", "Show live collaborators", "Panels", [
      "presence",
      "people",
    ]),
    add(
      "panel.diagnostics",
      "Open technical diagnostics",
      "Show secondary technical details",
      "Panels",
      ["diagnostics", "technical"],
    ),
    add(
      "panel.help",
      "Open studio help",
      "Show implemented tools and shortcuts",
      "Panels",
      ["help", "shortcuts"],
      true,
      "?",
    ),
    add(
      "panel.share",
      "Share board",
      "Copy an access-preserving board link",
      "Panels",
      ["share", "link"],
      input.ready,
    ),
    add(
      "theme.system",
      "Use system theme",
      "Follow this device",
      "View",
      ["theme", "system"],
      themePreference !== "system",
    ),
    add(
      "theme.light",
      "Use light theme",
      "Use the light appearance",
      "View",
      ["theme", "light"],
      themePreference !== "light",
    ),
    add(
      "theme.dark",
      "Use dark theme",
      "Use the dark appearance",
      "View",
      ["theme", "dark"],
      themePreference !== "dark",
    ),
    add(
      "selection.delete",
      "Delete selected object",
      "Delete the current object",
      "Selection",
      ["delete", "remove"],
      selected,
    ),
    add(
      "selection.hide",
      "Hide selected object in this view",
      "Hide only in this browser view",
      "Selection",
      ["hide", "local"],
      selected,
    ),
    add(
      "selection.lock",
      "Lock selected object in this view",
      "Lock only in this browser view",
      "Selection",
      ["lock", "local"],
      selected,
    ),
    add(
      "selection.rotate-clockwise",
      "Rotate selected object 15° clockwise",
      "Share an authoritative rotation",
      "Selection",
      ["rotate", "clockwise"],
      input.rotateAvailable,
    ),
    add(
      "selection.rotate-counterclockwise",
      "Rotate selected object 15° counterclockwise",
      "Share an authoritative rotation",
      "Selection",
      ["rotate", "counterclockwise"],
      input.rotateAvailable,
    ),
    add(
      "selection.reset-rotation",
      "Reset selected rotation",
      "Set the shared rotation to 0°",
      "Selection",
      ["rotate", "reset"],
      input.rotateAvailable,
    ),
  ];
  return commands.map((command) =>
    input.viewOnly &&
    (command.category === "Create" ||
      command.id === "tool.rectangle" ||
      command.id === "tool.sticky" ||
      command.category === "Selection")
      ? { ...command, available: false, disabledReason: "Use a larger screen to edit." }
      : command.available || command.category !== "Selection"
        ? command
        : { ...command, disabledReason: selectionReason },
  );
}
