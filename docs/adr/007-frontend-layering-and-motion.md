# ADR 007: Frontend layering and motion

Status: proposed
Date: 2026-08-13

## Context

The M2 web client combines a Konva scene and DOM chrome in one page. Its few overlays use unrelated
numeric z-index values, and the product has no formal focus, portal, motion, or reduced-motion model.
M3 adds panels, contextual tools, presence, notifications, a palette, dialogs, and terminal recovery
states. Without one ownership model, these surfaces can intercept canvas input, trap focus
incorrectly, cover terminal messages, or make synchronization appear complete before it is.

## Decision

Converge will use a two-domain renderer:

- Konva owns board-coordinate geometry, canvas hit testing, selection controls, guides, and remote
  board-coordinate adornments.
- DOM/CSS owns application chrome, semantic text, forms, menus, tooltips, status, panels, palettes,
  dialogs, and terminal overlays.

The workspace root uses `isolation: isolate`. A single exported semantic layer-token map owns every
z-index. Arbitrary numeric z-index values are prohibited by review and, if practical, lint/static
tests. The order is ambient, canvas, canvas controls, presence, contextual toolbar, chrome, panels,
popovers, notifications, palette, dialogs, terminal.

DOM overlays render into four workspace-owned portal roots:

- `overlay-popovers` for menus, tooltips, and context controls;
- `overlay-status` for nonmodal sync state and notifications;
- `overlay-modals` for palettes and dialogs; and
- `overlay-terminal` for revoked/recovery-blocked states.

An overlay coordinator owns open order, outside-pointer dismissal, focus trap and restoration, inert
background state, and Escape handling. Escape resolves terminal policy, dialog, palette, popover,
contextual selection, active tool, then selection. Terminal overlays are not dismissible into an
invalid session.

Pointer events default to none for decorative/background/guide/status containers. Only explicit
interactive descendants opt in. DOM contextual controls receive projected viewport bounds through a
narrow stage-coordinate adapter and do not reach into arbitrary Konva internals.

## Motion decision

Motion communicates causality and hierarchy; it never gates a command, database acknowledgement, or
recovery transition.

Allowed categories:

- micro feedback for hover, press, focus, selection, and status change;
- panel/popover/dialog entrance and exit;
- short object-toolbar repositioning after settled viewport change;
- bounded remote cursor interpolation; and
- reconnect/recovery status transitions that expose, rather than mask, authoritative state.

Initial tokens:

| Token      | Duration | Easing                        | Use                                                        |
| ---------- | -------- | ----------------------------- | ---------------------------------------------------------- |
| `instant`  | 0 ms     | linear                        | Authoritative correction, reduced motion, terminal fencing |
| `micro`    | 80 ms    | ease-out                      | Press and focus feedback                                   |
| `fast`     | 140 ms   | cubic-bezier(0.2, 0, 0, 1)    | Tooltip, menu, contextual controls                         |
| `standard` | 200 ms   | cubic-bezier(0.2, 0, 0, 1)    | Panel and status transitions                               |
| `emphasis` | 280 ms   | cubic-bezier(0.16, 1, 0.3, 1) | Dialog/palette entrance only                               |

Exits use `fast` unless focus safety requires immediate removal. Reduced motion uses opacity-only
micro feedback or no transition; remote cursors jump to the newest bounded sample. No continuous
ambient animation, editor blur, parallax, physics overshoot, or animation of authoritative sequence
progress is allowed.

## Consequences

- Layering, pointer ownership, focus, and Escape behavior are testable and reviewable.
- Konva remains optimized for spatial rendering while DOM retains accessibility semantics.
- Contextual DOM controls require a maintained coordinate adapter.
- Token enforcement adds foundation work before visual surface work.
- Product teams cannot solve overlap bugs by adding a larger z-index.

## Rejected alternatives

- All UI in Konva: weak semantic/accessibility and form/focus behavior.
- All objects in DOM: abandons the established scene renderer without evidence.
- One portal root without modal policy: makes Escape/focus ordering implicit.
- Ad hoc animation library usage: permits inconsistent timing and risks synchronization-obscuring
  motion.
