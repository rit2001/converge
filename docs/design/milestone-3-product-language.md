# Milestone 3 product language

Status: proposed; visual approval required before implementation.

## Direction: a calm shared studio

Converge should feel like a quiet, precise studio table: a warm neutral workspace, crisp instruments,
and small signals of other people working nearby. The identity is not a clone of Figma, Linear, Miro,
or a generic purple-gradient SaaS template. Durable collaboration is the product character: controls
feel responsive, saved state is legible, and recovery is composed rather than alarming.

Premium means fewer, better surfaces; consistent rhythm; excellent type; deliberate states; and no
visual effect without functional purpose.

## Token categories

Tokens use semantic names, not component names or raw values in components.

| Category         | Proposed system                                                                                                                                                                                                                   |
| ---------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Color            | Neutral “ink” and “paper” scales; one deep indigo-violet action hue; cyan/teal collaboration accents; amber recovery; red terminal; green reserved for verified success. Every semantic pair is contrast-tested.                  |
| Typography       | Humanist sans for product text, compact display weight for marketing headings, tabular numerals/monospace only for sequences and diagnostics. Type scale: 12, 13, 14, 16, 20, 24, 32, 48/56. External font choice needs approval. |
| Spacing          | 4 px base with 2, 4, 8, 12, 16, 20, 24, 32, 40, 48, 64 steps. Dense editor chrome uses 8/12; public surfaces breathe at 24–64.                                                                                                    |
| Radius           | 6 small, 10 control, 14 panel, 20 hero; pills only for statuses/tags. Canvas object radius remains object semantics, not global chrome fashion.                                                                                   |
| Borders          | One-pixel neutral boundaries, stronger focus/selection rings, no white-on-white border tricks. Hairlines adapt by theme.                                                                                                          |
| Elevation        | Four levels: inset/base, floating control, panel/popover, modal. Shadows are low-spread and paired with borders; canvas objects use a separate spatial shadow scale.                                                              |
| Surface material | Opaque or lightly translucent chrome only when contrast remains stable. Blur is optional for small floating chrome, forbidden across large editor panels and canvas content.                                                      |
| Iconography      | One coherent rounded-geometric SVG family with 1.75–2 px stroke; icons always have accessible names/tooltips. Unicode emoji/glyphs are removed from production chrome. Library choice needs dependency review.                    |
| Grid/background  | Static dot or cross grid with zoom-aware density and restrained major lines. No animated gradient, particle field, or decorative 3D editor background.                                                                            |
| Focus/selection  | Local focus uses a high-contrast dual ring. Selected objects use the action hue; collaborator selections use assigned presence colors plus non-color identity cues.                                                               |
| State            | Neutral syncing, amber degraded/retrying, green verified/saved, red revoked/terminal. Pending and recovered are distinct from error and success.                                                                                  |

## Theme strategy

Tokens must support light and dark from M3.1. Human approval chooses either:

1. ship both themes in M3 with system preference and explicit override; or
2. ship a fully polished light theme first while every component uses theme-safe tokens, then accept
   dark mode in M3.7 before milestone completion.

Theme switching must not reconstruct the board session or Konva stage. Persist only the preference,
not board data. Canvas object colors remain content and are not automatically inverted.

M3.1 resolves the initial delivery choice in favor of a polished light shipping theme. The semantic
token contract includes dark-compatible values, but no automatic activation or theme switcher is
shipped in this slice. The existing privacy-safe system font stack remains in use and no icon, font,
animation, or UI-framework dependency is added. Alpha is limited to focus rings, subtle selection,
shadows, and small floating chrome where the opaque fallback preserves contrast; large editor
surfaces and authoritative state indicators remain opaque.

## Surface language

### Public and board entry

- Lead with “shared thinking that survives the network,” demonstrated through ordered collaboration
  and reconnect recovery—not infrastructure jargon.
- Use real UI fragments or code-native diagrams rather than generated marketing imagery.
- Primary action is explicit: create a board or open a board link. Do not create durable data on page
  load.
- If server-backed recent boards are unavailable, label device-local recents honestly.

### Workspace

- Header prioritizes board name, collaborator presence, share action, and quiet sync state.
- Primary dock contains modes and creation tools. Contextual object properties appear near selection
  or in a deliberate side panel, not both without hierarchy.
- Layers panel uses thumbnails/icons, generated type labels, durable order, and clear local-versus-
  durable affordances.
- Diagnostics move behind a development/operator affordance and never expose IDs/hashes in ordinary
  product UI.

### Collaboration colors

Use a fixed accessible palette of 8–12 hues assigned by stable session-scoped hashing. Add initials,
patterns, or labels because color alone is insufficient. Presence color is ephemeral decoration, not
authorization or durable identity. Avoid assigning red, amber, and green state colors as ordinary
presence colors.

## Feedback copy

| Condition           | Preferred language                                                                                         | Avoid                                                                    |
| ------------------- | ---------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------ |
| Local pending       | “Saving…” / “Queued on this device”                                                                        | “Synced” before acknowledgement                                          |
| PostgreSQL ack      | “Saved”                                                                                                    | “Delivered everywhere”                                                   |
| Catch-up            | “Catching up with the board…”                                                                              | “Reloading your work”                                                    |
| Retry               | “Connection interrupted. Your queued edits are safe on this device.” when persistence evidence supports it | “All changes saved”                                                      |
| Recovered           | “Back in sync” with optional sequence-neutral details                                                      | Celebratory animation or raw hashes                                      |
| Revoked             | “You no longer have access to this board.”                                                                 | Raw user/board identifiers                                               |
| Recovery blocked    | “This board cannot be verified safely. Editing is disabled.”                                               | “Continue anyway,” silent snapshot skipping, or destructive reset advice |
| Persistence failure | “Local edit storage is unavailable. Editing is paused.”                                                    | Continuing optimism without persistence                                  |

### M3.5A synchronization language contract

The compact header status is derived only from current client lifecycle and pending-command evidence.
Priority is terminal/revoked or blocked recovery; missing current session; reconnect or locally
preserved pending changes; restoration; pending saves; then ready with zero pending commands. Its
bounded language is: “Connecting…”, “Restoring board…”, “Synced”, “Saving…”, “Reconnecting…”,
“Changes kept on this device”, “Access removed”, “Recovery needs attention”, and “Temporarily
unavailable”. “Synced” means only that this current session is ready with no local pending commands;
it does not claim global replica parity or delivery to collaborators. Terminal and recovery language
is sanitized, while technical identifiers and raw errors remain in the secondary diagnostics
disclosure.

### M3.5B presence language contract

The future ephemeral presence surface uses only “Live collaborators”, “Presence temporarily
unavailable”, “Active”, “Idle”, and “You”. It must never say “Everyone is synced”, “All
collaborators are online”, or imply exact membership, durable cursor history, or board-save
readiness. User IDs, presence-session IDs, and connection diagnostics are never product text.

Announcements are deduplicated and rate-limited. Ordinary successful commands do not create toasts.

## Motion language

- Hover/press/focus: 80–140 ms.
- Toolbars/popovers: 140 ms, small opacity/scale or directional movement.
- Panels: 200 ms, ≤16 px travel; canvas viewport does not shift until layout is known.
- Palette/dialog: 200–280 ms with immediate focus placement.
- Remote cursors: interpolate bounded samples only; never trail indefinitely.
- Reconnect: status icon/text transition, no infinite spinner when the system is waiting for a known
  retry deadline.
- Recovery corrections appear immediately or with a subtle crossfade; motion must not disguise an
  authoritative jump.

Reduced motion removes travel, interpolation, and nonessential crossfades while preserving state
labels and focus.

## Content and accessibility rules

- Sentence case, concrete verbs, and bounded explanations.
- No “magic,” “real-time guaranteed,” “exactly once,” or durability claims beyond M2 evidence.
- Every icon-only control has a visible tooltip and programmatic label.
- Error copy says what is safe, what is paused, and the next allowed action without exposing raw
  errors.
- Empty states teach one action and one shortcut; they do not become marketing panels inside the
  editor.

## Approval board

Before implementation, approve: wordmark/mark direction, core light palette, dark-theme timing,
font source, icon strategy, collaboration palette, editor density, panel placement, and phone
view-only policy. Approval should use static code-native mockups of landing, empty workspace,
selected object, reconnecting, and terminal recovery states.
