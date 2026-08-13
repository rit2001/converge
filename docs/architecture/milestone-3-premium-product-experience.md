# Milestone 3: Premium product experience

Status: proposed architecture and UX audit
Last reviewed: 2026-08-13
Baseline: `v0.2.0-m2`

## Definition of premium

For Converge, premium means restrained visual hierarchy, predictable spatial behavior, fast local
feedback, explicit durable-state feedback, and calm recovery under failure. It does not mean copying
another editor, hiding distributed state, or adding glass, glow, gradients, blur, and motion to every
surface. The interface should make collaboration feel immediate while making PostgreSQL
acknowledgement, distributed delivery, reconnect recovery, and fail-closed states understandable.

## Current-state inventory

### Product and route surface

| Area              | Current fact                                                                                                                          | Consequence                                                                                                                   |
| ----------------- | ------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------- |
| Routes            | The App Router has one `/` route. It immediately renders `Workspace`.                                                                 | There is no public landing, dashboard, onboarding, auth route, or route-level loading/error boundary.                         |
| Board entry       | A `?board=` query opens a board; otherwise the client creates “Product workshop” and replaces the URL.                                | Direct links work, but discovery, recent boards, naming, and explicit create/open intent do not.                              |
| Authentication    | Web requests rely on the configured API auth boundary. Local/test composition uses development identity assumptions.                  | There is no sign-in, account, session-expiry, or identity-switching experience. Production OAuth remains deferred.            |
| Board API         | Create board, fetch authorized board snapshot, bounded operation range, verified recovery, and owner-authorized member removal exist. | There is no board list/search, board rename route, member list, invite/add-member flow, share-link service, or version index. |
| Operational state | API health/metrics exist for operators, while `BoardStore` exposes detailed synchronization state to the client.                      | Product UX should use client lifecycle evidence, not call operational health endpoints from the editor.                       |

### Canvas and interaction surface

- Konva owns the stage, one object layer, rectangles, sticky notes, and one `Transformer`.
- The current tools are select and pan. The toolbar can create a rectangle or sticky and delete one
  selected object.
- Wheel zoom is clamped to 25–300%; pan is enabled only in pan mode. The viewport follows a
  `ResizeObserver`.
- Objects can be selected, dragged, and resized. Rotation is rendered but transformer rotation is
  disabled. There is no multi-selection, lasso, snapping, alignment guide, keyboard nudging,
  context menu, inline text/color editor, undo/redo, minimap, or zoom-to-fit.
- Rectangle and sticky are the only strict object kinds. Durable commands accepted end to end are
  `object.create`, `object.update`, `object.transform`, and `object.delete`.
- Render order follows `BoardState.order`, which is reconstructed from durable `stack_order`.
- The visible UI uses Unicode tool glyphs, a fixed development avatar, a raw connection label, an
  error toast, a zoom pill, and an opt-in diagnostics drawer containing identifiers and hashes.

### Collaboration and synchronization surface

M2 already provides strong internal state that the product barely presents:

- `BoardSessionController` owns replacement-session fencing and cancels stale work.
- `BoardTransport` distinguishes connecting, joining, catching up, retry waiting, ready,
  authorization failure, and error; it also bounds live buffering and retry backoff.
- The pending queue persists commands before optimism, submits in order, correlates acknowledgements,
  preserves identities across retry, and exposes idle/loading/ready/error recovery status.
- `BoardStore` separates committed state from optimistic pending state and maintains an
  authoritative state-hash lifecycle.
- Recovery verifies snapshot plus contiguous tail and atomically rebases before readiness.
- Membership revocation terminally fences the session.
- M2 provides at-least-once delivery. Duplicate transport evidence is normal and suppressed; the UI
  must never imply exactly-once transport.

The UI currently reduces this to a small status pill and raw diagnostics. It has no collaborator
presence, remote cursors/selections, per-command saving feedback, recovered-state explanation,
offline affordance, or graceful terminal overlay.

### Styling, responsive behavior, accessibility, and dependencies

- Styling is one global CSS file with hardcoded colors, radii, shadows, dimensions, and arbitrary
  z-index values. There is no Tailwind configuration or token system.
- The interface is light-only and uses the system fallback for “Inter”; no font is loaded.
- At widths below 700 px the connection pill is hidden and chrome margins shrink. The editor does
  not otherwise adapt.
- Buttons mostly depend on `title`; Unicode icons have no deliberate accessible names. Focus-visible
  treatment, landmarks beyond the existing header/aside/main, live-region status, canvas
  alternatives, dialog focus trapping, and reduced-motion policy are absent.
- `konva` and `react-konva` are the only visual-specialist dependencies. There is no component,
  accessible primitive, icon, animation, or visual-regression library.
- Tests cover store/transport/pending lifecycle thoroughly. Canvas tests cover authoritative
  geometry/rotation. Three Playwright flows cover two-client convergence, reconnect catch-up, and
  stacking stability. There are no keyboard, accessibility, responsive, theme, screenshot, or
  performance acceptance tests.

### Risks and missing flows

- `Workspace` subscribes to the whole Zustand store, so any state transition can rerender the whole
  chrome and canvas boundary.
- Every object is reconciled through one React/Konva layer; there is no scene partitioning,
  viewport culling, drag preview boundary, or remote-presence layer.
- Transform submission occurs only on drag/transform end, which is correct for network load, but
  no explicit local-preview versus durable-commit feedback exists.
- The diagnostics drawer serializes all committed visible objects into the DOM and is unsuitable as
  a default product surface at larger object counts.
- Board creation on first visit is an implicit mutation. There is no confirmation, dashboard, or
  recoverable invalid-link flow.
- Missing product flows include board discovery/naming, real authentication, member discovery and
  invitation, durable history/version browsing, restore, undo/redo, presence, comments, export, and
  administrative recovery guidance.

## Target information architecture

| Surface                   | Responsibility                                                                                                  | M3 boundary                                                         |
| ------------------------- | --------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------- |
| Public landing            | Explain collaborative durability with an interactive-but-nondestructive product story and clear entry action.   | No generated imagery or decorative 3D scene.                        |
| Authentication/onboarding | Explain identity boundary, request sign-in when a real provider exists, and guide first board creation/opening. | Provider selection is a human decision; no fake production auth.    |
| Board entry/dashboard     | Create a named board, open a known link, show locally remembered recent links where honest.                     | Server-backed listing/search needs a new authorized API.            |
| Workspace                 | Compose board header, canvas, tools, layers, contextual controls, sync feedback, and terminal states.           | Must preserve M2 session ownership and transport boundaries.        |
| Share/members             | Copy the current URL and explain roles; show/manage members only after list/add APIs exist.                     | Never infer members from presence or expose IDs as product UI.      |
| Version/recovery          | Explain current recovery state and last verified synchronization; browse/restore only with new durable APIs.    | The operational recovery endpoint is not a version browser.         |
| Command palette/help      | Search tools and safe frontend commands; expose shortcuts and disabled reasons.                                 | Commands cannot bypass authorization or durable command validation. |

Suggested route ownership is `/` for landing, `/boards` for board entry, and `/boards/[boardId]` for
the editor. Exact routing and whether the root redirects for signed-in users require product approval.

## State and feedback model

The interface must represent four different facts without collapsing them:

| Phase                     | Evidence                                                                                          | User feedback                                                            | Prohibited implication                      |
| ------------------------- | ------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------ | ------------------------------------------- |
| Optimistic local          | Command is persisted locally and applied to optimistic projection.                                | Immediate object motion plus a subtle pending mark.                      | “Saved” or visible to collaborators.        |
| PostgreSQL acknowledged   | Successful operation acknowledgement with committed sequence.                                     | “Saved”/check state; reconcile geometry if authoritative result differs. | Delivered to every collaborator.            |
| Distributed live delivery | Current session consumes ordered live evidence; peer sessions may see it later or via catch-up.   | Presence/activity may settle; no noisy toast per operation.              | Exactly-once delivery.                      |
| Recovery/reconciliation   | Verified snapshot and contiguous tail applied, pending queue preserved, current generation ready. | Calm “Back in sync” confirmation and optional details.                   | Silent data discard or unverified recovery. |

Product states map as follows:

- `connecting`/`joining`: bounded initial loader, canvas controls disabled until initialized;
- `catching-up`: retain safe visible state, prevent conflicting commands, announce “Catching up”;
- `retry-wait`: show offline/reconnecting status and whether local edits are safely queued;
- `ready`: quiet synchronized state, not a permanent green banner;
- pending persistence error: stop accepting commands and provide retry/export guidance without
  claiming server durability;
- `authorization-failed`/revoked: terminal modal/overlay, no editor interaction, safe navigation;
- `RECOVERY_BLOCKED`: terminal recovery surface with no legacy fallback or “continue anyway” action;
- replacement session: old callbacks remain visually and logically silent.

## Workspace layering architecture

All layer values come from one semantic token map. Components may request a token but may not use a
numeric `z-index` literal. Each major DOM surface creates a documented stacking context with
`isolation: isolate`; transforms on shell ancestors are prohibited unless the token owner requires
one.

| Order/token            | Surface                                                      | Owner                                                                            | Pointer/focus policy                                                                              |
| ---------------------- | ------------------------------------------------------------ | -------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------- |
| `ambient` / 0          | Workspace base color and static grid surround                | DOM/CSS                                                                          | No pointer events or animation.                                                                   |
| `canvas` / 100         | Infinite grid, canvas objects                                | Konva stage/layers                                                               | Stage handles pan/zoom; objects handle hit testing.                                               |
| `canvasControls` / 200 | Selection transformer, snapping/alignment guides             | Konva overlay layers                                                             | Guides ignore events; handles accept pointer input.                                               |
| `presence` / 300       | Remote cursors and collaborator selection outlines           | Prefer Konva for board coordinates; DOM only for labels requiring rich semantics | Never blocks local object interaction.                                                            |
| `objectToolbar` / 400  | Contextual selected-object toolbar                           | DOM portal anchored from projected canvas bounds                                 | Interactive only while selection is current; closes before underlying tool change.                |
| `chrome` / 500         | Primary tool dock, board header, zoom controls               | DOM                                                                              | Application keyboard navigation and explicit accessible labels.                                   |
| `panels` / 600         | Layers, members, history side panels                         | DOM                                                                              | Resizable within bounds; canvas remains operable when policy permits.                             |
| `popover` / 700        | Menus, tooltips, color controls, context menus               | DOM portal `#overlay-popovers`                                                   | Dismiss on outside pointer and Escape; no focus trap for nonmodal popovers.                       |
| `notification` / 800   | Sync status, toasts, nonmodal recovery confirmation          | DOM portal `#overlay-status`                                                     | `pointer-events: none` container; actionable children opt in. Live-region output is deduplicated. |
| `palette` / 900        | Command palette                                              | DOM portal `#overlay-modals`                                                     | Modal focus trap; restores trigger focus.                                                         |
| `dialog` / 1000        | Share/members, onboarding, destructive confirmation          | DOM portal `#overlay-modals`                                                     | Modal focus trap and inert background.                                                            |
| `terminal` / 1100      | Revoked, recovery-blocked, unrecoverable persistence overlay | DOM portal `#overlay-terminal`                                                   | Highest focus owner; canvas/chrome inert; cannot be dismissed into an invalid session.            |

Escape precedence is terminal policy (non-dismissible) → top dialog → palette → popover/context menu →
contextual toolbar selection → active drawing tool → selection. A centralized overlay coordinator
owns this order, focus restoration, scroll locking, and portal registration.

Konva owns board-coordinate visuals and hit testing. DOM owns application chrome, text semantics,
forms, focus, menus, and modal behavior. Coordinate conversion is a narrow adapter from stage world
bounds to viewport bounds; DOM controls do not read arbitrary Konva internals.

## Durable Layers-panel capability matrix

The durable-event enum contains reserved names that are not accepted by `durableCommandSchema`; a
name in that enum is not implementation evidence.

| Capability                  | Classification                       | Evidence and M3 policy                                                                                                                                                                   |
| --------------------------- | ------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Ordered object list         | Already supported                    | `BoardState.order`, snapshot `stackOrder`, and unique database `stack_order` provide authoritative ordering. Render a derived list without duplicating state.                            |
| Selection synchronization   | Frontend-only                        | Local selected ID already exists. Panel/canvas selection can share it. Remote selection needs presence evolution.                                                                        |
| Drag-to-reorder             | Requires protocol/database evolution | `object.reorder` is reserved but has no accepted command schema/reducer/repository path. Add an ordered durable command before enabling drag.                                            |
| Bring forward/send backward | Requires protocol/database evolution | Same durable reorder prerequisite; do not mutate local array as if saved.                                                                                                                |
| Rename                      | Requires protocol/database evolution | Canvas objects have no name field. A generated local label is frontend-only and must not be presented as durable. Board rename also lacks an API.                                        |
| Hide/show                   | Frontend-only                        | No hidden field exists. A local visibility filter may be offered only when labeled “This view”; a durable variant would require evolution.                                               |
| Lock/unlock                 | Frontend-only                        | No lock field or authorization semantics exist. Local hit-test suppression is not collaboration locking; a durable variant would require evolution.                                      |
| Grouping                    | Intentionally deferred               | `group.create`/`group.ungroup` are reserved only; there is no group model, reducer, persistence, or transform semantics. Any future implementation requires protocol/database evolution. |

Durable reorder must define concurrent reorder semantics, idempotency, snapshot representation, and
compaction/recovery behavior. Durable hide, lock, name, and group work requires a separate protocol
and migration review and is not silently bundled into visual polish.

## Interaction model

- Pan with hand tool, Space-drag, or middle pointer; zoom about pointer with wheel/trackpad and
  explicit controls. Prevent browser zoom only inside the active canvas boundary.
- Single selection ships before lasso/multi-selection. Multi-selection requires a tested selection
  model and batch-command policy; it must not submit one network request per pointer frame.
- Drag/resize/rotate provide local preview at frame rate and submit one bounded transform at the
  interaction boundary. Rotation can be enabled only after tests cover geometry and recovery.
- Snapping runs locally against visible candidate bounds; guides are ephemeral and never durable.
- Tool switching is explicit, shortcut-visible, and input-aware. Shortcuts do not fire while typing.
- Context menu and palette invoke the same command registry, including authorization/readiness and
  selection predicates.
- Undo/redo is not a local stack rewind in a collaborative board. It requires compensating durable
  commands, current-target validation, and product-approved conflict semantics.
- Layer actions that change durable order wait for the accepted reorder contract.
- Remote cursors/selections are ephemeral, rate-limited, and never used as membership truth.

## Accessibility and responsive policy

- Target WCAG 2.2 AA: 4.5:1 normal text, 3:1 large text and meaningful UI graphics, and visible
  focus indicators with at least 3:1 adjacent contrast.
- All application chrome, panels, menus, palette, and dialogs are keyboard reachable with semantic
  names and deterministic focus order. Do not rely on `title` or color alone.
- Status changes use deduplicated `aria-live` announcements: connecting, queued locally, saved,
  reconnecting, recovered, revoked, and terminal recovery failure.
- Provide an accessible object list/layers alternative for selection and object actions. The canvas
  bitmap itself is not treated as a complete accessibility tree.
- Honor `prefers-reduced-motion`; eliminate interpolation and transform travel where motion is not
  essential. High contrast and 200% browser zoom must preserve controls and terminal messages.
- Minimum pointer target is 44×44 CSS px for touch contexts and 36×36 for dense desktop chrome with
  44 px effective spacing where practical.
- Policy proposal: desktop is the full editor; tablet supports core select/pan/create/transform with
  touch-safe panels; phone is view/comment/status only until mobile editing receives dedicated
  interaction testing. Whether phone view-only is acceptable requires human approval.

## Performance budgets and acceptance

| Budget           | Initial gate                                                                                                                                    | Measurement                                                             |
| ---------------- | ----------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------- |
| Pan/zoom/drag    | 60 FPS target; p95 frame ≤16.7 ms in ordinary boards, no repeated frame >50 ms                                                                  | Browser Performance panel and Playwright trace on production build.     |
| Input feedback   | Local pointer-to-preview p95 ≤50 ms; tool/button feedback ≤100 ms                                                                               | Performance marks around pointer handler and next paint.                |
| React boundaries | Pointer movement must not rerender workspace chrome; object change rerenders only affected scene/list rows where feasible                       | React Profiler commit counts with synthetic interactions.               |
| Object tiers     | 100, 500, 1,000 visible objects; correctness at all tiers, 60 FPS target at 100/500 and measured degradation documented at 1,000                | Deterministic fixture and trace; no unsupported capacity claim.         |
| Remote cursor    | Coalesce to one visual update per animation frame; network send ≤20 Hz per collaborator, bounded interpolation buffer                           | Fake clock tests and multi-client trace.                                |
| Animation        | ≤3 concurrent chrome transitions; no continuous editor background or canvas blur                                                                | Reduced-motion and animation audit.                                     |
| JavaScript       | M3 route first-load JS budget ≤250 KiB gzip, editor-specific incremental JS ≤180 KiB gzip, exceptions require recorded approval                 | Next build output/bundle analyzer if added in a reviewed tooling slice. |
| Route load       | Cached shell interaction target ≤1 s on reference desktop; cold local p95 ≤2.5 s with documented topology                                       | Playwright navigation timing, not an SLA.                               |
| Long tasks       | No task >100 ms during ordinary interaction; fewer than 2 tasks >50 ms in a 10-second drag trace                                                | PerformanceObserver/trace.                                              |
| Cleanup          | Board/session replacement releases Konva nodes, observers, global listeners, timers, pending request controllers, portals, and presence buffers | Repeated replacement tests and heap/listener snapshots.                 |
| Pointer network  | Zero requests per raw pointer event. Durable commands occur at interaction boundaries; presence is explicitly batched/rate-limited.             | Request counter in interaction tests.                                   |

Budgets are engineering acceptance targets, not measured capacity claims. Reference hardware,
browser version, fixture seed, viewport, DPR, and production build must accompany measurements.

## Testing strategy

- Component tests: token use, state badges, layers derivation, command predicates, panel/dialog behavior.
- Interaction tests: pan/zoom, selection, transform, snapping, reorder once durable, context menu, and
  no request per pointer movement.
- Keyboard/accessibility: tab order, shortcuts outside inputs, Escape precedence, focus trap/restore,
  live regions, axe-style automated checks plus manual screen-reader review.
- Playwright: landing → board entry → workspace; two-user collaboration; reconnect/recovered;
  revocation and recovery-blocked terminal states; pending persistence failure.
- Visual regression: deterministic fonts/data/DPR, light/dark, reduced motion, desktop/tablet/phone,
  and selected/reconnecting/terminal states. Store small reviewed screenshots; mask nondeterministic
  cursor/time regions.
- Performance: production-build traces at object tiers and multi-user presence rates; fail only on
  agreed deterministic budgets.
- Browser matrix: current Chromium in every PR; current Firefox and WebKit in M3 final acceptance.

## Human approvals required

1. Original visual direction and wordmark treatment before token implementation.
2. Typography source/licensing and whether any external font request is acceptable.
3. Full dark theme in M3 versus light foundation plus token readiness.
4. Desktop/tablet/phone policy, especially phone view-only.
5. Authentication provider and whether dashboard recent boards may be local-only before a list API.
6. Durable reorder conflict semantics and whether it is included in M3.4.
7. Scope/timing of member list/invite, presence protocol, and version browse/restore APIs.
8. Screenshot-baseline review process and reference environments for performance budgets.

## Explicit non-goals

- Weakening M2 ordering, recovery, readiness, authorization, or at-least-once semantics.
- Decorative Three.js, continuously animated editor backgrounds, or expensive canvas blur.
- Production deployment, provider dashboards, capacity benchmarking, or benchmark reruns.
- Claiming presence, history, undo/redo, grouping, lock, hide, or rename is durable before its
  protocol and storage contract exists.
- A 10,000-user, multi-replica performance, exactly-once, or Redis-durability claim.

## Definition of M3 completion

M3 is complete when the approved product surfaces use one tokenized visual system; the workspace
follows the layered ownership model; core canvas and layers interactions are accessible and tested;
collaboration and synchronization states distinguish local, acknowledged, delivered, and recovered
evidence; responsive/theme policy is implemented; performance budgets have reproducible evidence;
and Chromium/Firefox/WebKit acceptance passes. Any deferred durable capability remains visibly
absent or truthfully labeled, never simulated as collaborative state.
