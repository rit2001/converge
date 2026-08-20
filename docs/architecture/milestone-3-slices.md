# Milestone 3 implementation slices

Status: proposed. Each slice is independently reviewable and must preserve all M2 correctness and
privacy guarantees. Commands are run from the repository root. New dependencies require explicit
approval and are not implied by this plan.

## Global gates

Every slice starts from the accepted prior slice with a clean tree. It must keep stale-session
fencing, persist-before-optimism, ordered acknowledgements, verified recovery, terminal revocation,
and fail-closed readiness intact. UI language may not claim exactly-once delivery, Redis durability,
or server persistence before acknowledgement.

Stop a slice for an accessibility regression, an unexplained transport/store failure, arbitrary
z-index or raw style proliferation, an unapproved dependency, a protocol implication not covered by
a strict schema, or a measurable breach of its performance budget.

## M3.1 — Design tokens and frontend foundations

**Preconditions**

- Product language, light palette, typography source, icon strategy, and theme timing are approved.
- ADR 007 is accepted.

**User-visible invariant**

The existing workspace renders with one coherent, accessible visual foundation without changing any
board/session behavior.

**Scope**

- Establish semantic color, typography, spacing, radius, border, elevation, motion, focus, and
  z-index tokens.
- Add workspace portal roots and overlay coordinator primitives.
- Add accessible button/icon/tooltip, status badge, surface, and focus primitives.
- Split global CSS into explicit foundation ownership while retaining App Router compatibility.
- Add reduced-motion primitives and light/dark-ready token structure; ship dark only if approved.

**Likely files**

- `apps/web/app/layout.tsx`, `apps/web/app/styles.css`
- `apps/web/src/ui/*`, `apps/web/src/styles/*`, `apps/web/src/overlays/*`
- component/token tests and optional static token validation

**Tests**

- Token completeness and no arbitrary layer values.
- Portal ownership, Escape precedence, focus restoration, reduced-motion resolution.
- Existing 81 web tests unchanged in semantics.

**Verification**

```text
pnpm --filter @converge/web test
pnpm --filter @converge/web typecheck
pnpm --filter @converge/web lint
pnpm build
pnpm exec prettier --check <changed-files>
git diff --check
```

**Commit**

`feat(web): establish premium design foundations`

**Explicit exclusions**

Landing content, route restructuring, canvas feature changes, presence, durable commands, and
unapproved component/icon/motion dependencies.

**Stop condition**

Tokens cannot meet AA contrast, overlay focus/Escape ownership is ambiguous, or a dependency is
required without approval.

## M3.2 — Premium landing and board-entry experience

**Preconditions**

- M3.1 accepted.
- Root-route and authentication-boundary decisions approved.

**User-visible invariant**

A visit never creates data implicitly: people deliberately create a named board or open an
authorized board link and receive a clear unavailable/authentication state.

**Scope**

- Add public landing and explicit board-entry routes.
- Add create/open forms using existing create and fetch APIs.
- Add honest device-local recents only if approved and clearly labeled.
- Add route loading, invalid-link, unauthorized, empty, and retry states.
- Use code-native product demonstrations; no generated marketing imagery.

**Likely files**

- `apps/web/app/page.tsx`, `apps/web/app/boards/*`, `apps/web/app/boards/[boardId]/*`
- `apps/web/src/entry/*`, route/component tests, `tests/playwright/*`

**Tests**

- No POST on landing/load.
- Explicit create, open known board, invalid/forbidden link, retry, keyboard form, and focus tests.
- Playwright landing → create/open → workspace.

**Verification**

Web test/typecheck/lint, root build, targeted Playwright entry flows, Prettier, and diff check.

**Commit**

`feat(web): add premium board entry experience`

**Explicit exclusions**

Production OAuth, server-backed board list/search, invitations, billing, analytics, and deployment.

**Stop condition**

The design requires pretending a development identity is production auth or needs an unapproved
board-list API.

## M3.3 — Layered workspace shell

**Preconditions**

- M3.1 accepted; editor route from M3.2 stable.
- Workspace layer table and desktop panel placement approved.

**User-visible invariant**

Header, tool dock, canvas, contextual controls, panels, notifications, and terminal surfaces stack
and focus deterministically without intercepting the wrong canvas interaction.

**Scope**

- Decompose the monolithic `Workspace` into session, chrome, canvas, panel, status, and terminal
  boundaries.
- Implement semantic layers/portals and overlay coordinator.
- Replace raw diagnostics with a development-only details surface.
- Add board header, accessible tool dock, sync summary, panel host, and terminal revocation/recovery
  overlay using existing store evidence.
- Subscribe to narrow Zustand selectors so unrelated chrome does not rerender on pointer movement.

**Likely files**

- `apps/web/src/components/workspace.tsx`, `apps/web/src/workspace/*`
- `apps/web/src/components/canvas.tsx`, `apps/web/src/overlays/*`, `apps/web/app/styles.css`

**Tests**

- Layer order, pointer pass-through, focus/Escape precedence, terminal inert state, selector/render
  boundaries, session replacement cleanup.
- Existing convergence/reconnect Playwright flows.

**Verification**

Web tests/typecheck/lint, root build, relevant Playwright flows, interaction trace smoke, Prettier,
and diff check.

**Commit**

`feat(web): compose layered workspace shell`

**Explicit exclusions**

New durable commands, presence transport, member/history APIs, and visual-regression tooling.

**Stop condition**

Any terminal surface can be bypassed, old sessions can update current UI, or arbitrary layer values
remain necessary.

## M3.4 — Canvas tools, selection, snapping, and Layers panel

**Preconditions**

- M3.3 accepted.
- Single versus multi-selection scope and durable reorder decision approved.

**User-visible invariant**

Canvas and Layers panel reflect one authoritative object order and selection model; local previews
remain immediate while durable mutations occur only at bounded interaction boundaries.

**Scope**

- Add precise pan/zoom controls, keyboard nudge, selected-object properties, rotation, snapping, and
  alignment guides using current transform/update commands.
- Add accessible Layers list derived from `BoardState.order`, synchronized local selection, generated
  type labels, and session-local hide/lock only if explicitly labeled “This view.”
- Add drag reorder/forward/back only after a separately reviewed strict durable reorder contract,
  reducer, repository, snapshot, recovery, and concurrency policy exists.
- Multi-selection ships only with approved batch/partial-failure semantics.

**Canvas grid and snapping contract**

The M3 canvas grid is an infinite 22-by-22 world-unit lattice anchored at `(0, 0)`; viewport pan and
zoom only change its presentation. Drag snapping aligns an object's leading edge to that lattice and
aligns leading edge, center, and trailing edge against the logical axis-aligned bounds of visible
objects. The tolerance is 8 CSS screen pixels, converted to world units by dividing by current
viewport scale. Axes resolve independently, Option/Alt bypasses snapping for the current pointer
sample, and resize snapping is out of scope. Per-axis candidates choose the smallest screen-space
adjustment; exact ties prefer visible-object references over grid, then lexical object identity,
reference anchor, moving anchor, and signed adjustment. Object identities are implementation-only and
never appear in product or accessibility UI.

Rotation uses the existing authoritative `object.transform.rotation` field. UI-originated angles are
finite, normalized into `[0, 360)`, and canonicalize negative zero to zero. Pointer rotation is free
by default; holding Shift rounds its local preview to the nearest 15 degrees. Rotation never invokes
positional snapping, and exactly one transform command is emitted only when a valid rotation gesture
ends.

**Likely files**

- `apps/web/src/components/canvas.tsx`, `apps/web/src/canvas/*`, `apps/web/src/layers/*`
- `apps/web/src/board-store.ts` only for explicit frontend selection/view state
- If approved, protocol/canvas-engine/database/API files in a separate prerequisite commit

**Tests**

- Pan/zoom bounds, keyboard input guards, transform preview/one-submit boundary, snapping geometry,
  rotation recovery, panel order/selection, local hide/lock labeling, no request per pointer event.
- Durable reorder conflict/replay/recovery tests if that contract is approved.

**Verification**

Protocol/canvas/web tests as affected, integration only if durable evolution occurs, Playwright canvas
interaction flows, production build, profiling at 100/500 objects, Prettier, and diff check.

**Acceptance status (2026-08-20)**

Complete. A bounded two-context Playwright acceptance flow creates supported objects through the UI;
checks Canvas/Layers selection, authoritative stacking order, keyboard activation and focus return;
proves local hide/lock isolation and reload recovery; exercises snapped and Option/Alt-bypassed drags;
and verifies authoritative accessible rotation convergence. It also checks 1440×900, 1024×768, and
390×844 layouts plus forced-colors and reduced-motion media modes. This acceptance adds no
multi-selection, undo/redo, grouping, durable reorder/rename, or M3.5+ capability.

**Commit**

`feat(web): refine canvas tools and layers`

If durable reorder is approved, it must be an earlier separate commit such as
`feat(protocol): add durable object ordering`.

**Explicit exclusions**

Durable rename/hide/lock/grouping, local-stack undo, and unreviewed multi-object command fan-out.

**Stop condition**

UI implies a local action is durable when no contract exists, sends per-frame commands, or loses
authoritative order after reload/recovery.

## M3.5 — Collaboration presence and synchronization UX

**Preconditions**

- M3.3 accepted.
- Presence identity, privacy, rate, expiry, and reconnect semantics approved in a protocol design.

**User-visible invariant**

People can distinguish collaborators’ ephemeral activity from durable board state and always know
whether their own edit is local, acknowledged, reconnecting, recovered, or terminally blocked.

**Scope**

- Build pending/saving/saved/reconnecting/catching-up/recovered status from existing lifecycle state.
- Add remote cursors/selections only through an approved ephemeral Socket.IO contract with bounded
  send rate, expiry, interpolation, and cleanup.
- Add collaborator presence summary that never serves as membership authorization evidence.
- Add deterministic revoked and `RECOVERY_BLOCKED` acceptance UX.

**B1/B2/B3 presence breakdown**

- **M3.5B1 — architecture and protocol (complete):** Separate strict, versioned lossy-presence
  schemas and event maps from durable delivery. Presence uses the proposed `converge:presence:v1:*`
  Redis key namespace and `converge:presence:v1:pubsub` Pub/Sub channel, with per-API publisher and
  subscriber resources; no Socket.IO Redis adapter, durable stream, consumer cursor, PostgreSQL,
  outbox, snapshot, recovery, IndexedDB, or compaction participation. Redis ACLs grant the presence
  principal only bounded key and Pub/Sub access in that namespace, never delivery-stream access.
  Startup/reconnect failure is reported as presence unavailable and never changes HTTP/socket editing
  readiness; shutdown closes the dedicated subscriber then publisher and expires only locally-owned
  session keys. B2 must implement bounded 20 Hz cursor publication, 15 s heartbeat, 45 s TTL, 30 s
  idle transition, 100-session snapshots, immediate graceful leave, and TTL crash cleanup without
  keyspace notifications. Capacity admission disables only that socket's presence.
- **M3.5B2 — API/Redis ephemeral plane:** Authorize board admission from the authenticated socket,
  generate presence-session UUIDs server-side, write expiring records, publish deltas, read bounded
  late-join snapshots, and preserve session-generation fencing. It must remain fully isolated from
  durable editing and delivery readiness.

**M3.5B2A transport status (complete)**

The standalone `RedisPresenceTransport` uses three separately owned Redis connections (command,
publisher, subscriber) and does not compose into `buildApp` or Socket.IO. For board `B` and presence
session `S`, it stores JSON state at `converge:presence:v1:{B}:session:S`, a ZSET expiry index at
`converge:presence:v1:{B}:sessions`, and a bounded TTL tombstone at
`converge:presence:v1:{B}:tombstone:S`; braces keep all board mutation keys in one cluster slot.
Atomic Lua admission/refresh/leave scripts obtain Redis `TIME`, enforce the 100-active-session limit,
increment safe revisions, update the index/record TTL, and create revisioned leave tombstones. A
bounded snapshot script drops logically expired index members and returns at most 100 strictly
validated full states. Physical records and inactive indexes expire by TTL; no keyspace notifications
are used. Publishing a validated full upsert/leave delta to `converge:presence:v1:pubsub` happens
only after the atomic record change, so publish failure cannot corrupt session storage and maps only
to presence availability.

**M3.5B2B1 runtime status (complete)**

`PresenceRuntime` is independently supervised behind `API_PRESENCE_ENABLED=false` by default.
When enabled with the existing valid Redis URL, one API instance owns one presence transport and
runtime. It binds an authenticated socket only after the durable board join/catch-up acknowledgement,
then asynchronously admits a server-generated presence session. Redis admission, refresh, publish,
or recovery failures emit only bounded presence-unavailable evidence: they never delay a join or
change HTTP readiness, socket-editing readiness, delivery consumption, acknowledgement, recovery,
or pending-command behavior. Disabled deployments create no Redis presence resources and still
advertise the unavailable presence capability. Per-binding generations fence replaced, revoked,
disconnected, and stopped sockets before timers or idempotent leave work can act.

**M3.5B2B1 correction — Redis reconnect supervision (complete)**

Each `RedisPresenceTransport` owns a single reconnect supervisor for its three fresh node-redis
clients (command, publisher, subscriber). Node-redis reconnect remains disabled. The supervisor
attempts immediately, then retries with full jitter from 0 through `min(250ms × 2^attempt, 10,000ms)`
until terminal stop; the attempt count resets only after all three clients connect and the subscriber
has subscribed to the strict presence channel. A connection generation is available only after that
complete cycle. A client error/end or a proven command/publisher failure fences that generation,
closes all its clients, emits one bounded unavailable transition, and schedules at most one retry.
Retired generation callbacks and subscriber messages are ignored. Recovery emits available once so
the runtime can re-admit current bindings and send fresh snapshots. This remains a presence-only
supervisor and does not alter HTTP, Socket.IO editing, or durable delivery readiness. M3.5B2B2
multi-instance acceptance remains pending.

**M3.5B2B2 multi-instance acceptance (complete)**

The final bounded topology uses one disposable migrated PostgreSQL database, one real Redis instance,
and two independently production-composed local-delivery APIs on dynamic ports. Each API owns its
three presence Redis clients; Socket.IO retains its local default adapter, and no delivery stream,
consumer group, or in-process cross-API bridge participates. Real authenticated owner/editor sockets
plus an owner second tab join one board, while a viewer joins another board. The acceptance proves
late snapshot contents, cross-replica Pub/Sub upserts, session-level multi-tab wire evidence,
primary-board room isolation, strict malformed/wrong-board rejection, rate-coalesced cursor final
state, and revision advancement. Killing only API A's owned Redis presence connection emits bounded
unavailable without changing local HTTP/socket editing availability; an authoritative PostgreSQL
command still acknowledges. Fresh A Redis clients subscribe, current bindings re-admit with fresh
snapshots, and cross-replica updates resume. Explicit disconnect yields one effective leave while the
other tab remains. It deliberately does not duplicate the separate 45-second TTL crash-expiry contract
with a long acceptance wait. This acceptance makes no concurrency or capacity claim; B3 frontend UX
remains pending.

**M3.5B self-session correction (complete)**

Every socket-specific `board:presence-snapshot` now includes required internal
`selfPresenceSessionId`. The strict snapshot schema requires exactly one participant with that ID;
that participant is the sole authoritative source of the receiving client's self user identity and
display name. Redis storage snapshots deliberately omit this socket-specific field. After admission,
the runtime verifies that the returned admitted participant occurs exactly once in the bounded storage
snapshot with matching authenticated principal evidence, then adds the session ID and emits the
snapshot before available. A mismatch emits only presence unavailable and never affects joining or
editing. Recovery uses the current fresh binding session, so replacement snapshots deterministically
replace self evidence; other same-user sessions remain valid multi-tab evidence. The ID is protocol
evidence only and must never become a visible, accessible, telemetry, diagnostic, or log label.

**M3.5B3A client presence state (complete)**

`PresenceStore` is a separate, generation-owned ephemeral authority; it never enters BoardStore,
pending persistence, recovery, hashes, or durable commands. Strict self-specific snapshots establish
the self user only by resolving `selfPresenceSessionId`. Greater per-session revisions replace state;
equal/lower evidence is ignored, and revisioned leaves create at-most-200 tombstones retained for two
presence TTL windows so delayed upserts cannot resurrect sessions. At most 100 sessions, one
earliest-expiry timer, and one outbound coalescing timer are retained. Server observation/expiry
durations are translated at receipt time, so last-known unavailable evidence disappears on logical
expiry. The selector groups by user ID, keeps same-user tabs internally, selects active/newest/greatest-
revision/canonical-session cursor deterministically, and labels only the authenticated group “You.”
Outbound evidence is strict `presence:update` only, deduplicated and limited to 20 Hz with latest-value
coalescing; it never uses a durable command path. BoardTransport registers strict presence listeners
before connection, fences callbacks by its current board session, and clears the store/listeners at
terminal close.

**M3.5B3B premium roster and cursor UX (complete)**

The workspace presents at most four generated-initial avatars in the header, with the authenticated
group first, active peers before idle peers, and a bounded overflow count. Its portal panel uses only
display names, “You”, Active/Idle, and a useful tab count; unavailable evidence is explicitly labelled
“Presence temporarily unavailable”, and expired last-known evidence disappears with the store. Canvas
cursors render only current available active remote groups in a non-interactive Konva presence layer.
They are culled outside the transformed viewport, use palette tokens, and share a single short
requestAnimationFrame interpolation loop; reduced-motion and large jumps render immediately. Mouse and
pen coordinates are converted through the stage pan/zoom transform and passed to the existing 20 Hz
publisher; leave, blur, hidden documents, cancellation, and unmount clear with cursor null. Presence
does not affect synchronization or editing readiness. Final multi-client browser acceptance remains pending.

**M3.5C1 dual-replica browser topology (complete)**

The test-owned Playwright harness creates a disposable migrated PostgreSQL database and a bounded Redis
presence plane, then composes API A/API B with separate authenticated development principals and dynamic
listeners. Two direct Next CLI processes run with distinct validated `.next-m35c-*` output directories and
distinct compiled `NEXT_PUBLIC_API_URL` values; no source, `.env`, production API routing, Socket.IO adapter,
or in-process presence bridge is changed. It supplies browser A, a separate A tab, and B for one repository-
authorized board, captures only Socket.IO wire evidence for self-session assertions, and fences/cleans every
browser, process, database, Redis key, and test directory on success or a forced partial startup failure.
M3.5C visual acceptance is complete. One serial final browser topology reuses those exact API/web/browser
owners to prove A/A2/B self-session grouping, independent “Synced” presentation, a PostgreSQL-acknowledged
operation, Redis Pub/Sub cursor routing and cursor-null clearing, keyboard roster focus recovery, responsive
header behavior, and API-A-only Redis presence interruption/recovery. The test-owned Redis `CLIENT KILL`
boundary observes unavailable then a fresh self-specific snapshot and resumed cross-replica cursor evidence;
it does not add a production recovery hook. Presence remains independent from editing readiness and durable
operation counts. The acceptance makes no capacity, load, exact-membership, or exact-presence claim.

- **M3.5B3 — premium roster and cursor UX:** Group valid sessions by authenticated user (one avatar;
  self is “You”), use the most recently active session cursor, tolerate unavailable presence, and
  render reduced-motion/accessible cursors without exposing user or session IDs.

**Presence ordering contract**

Every upsert carries complete participant state and a monotonically increasing per-session revision.
Clients retain only the greatest revision per presence-session ID; snapshot/live overlap is resolved
the same way. Leave or expiry carries its own revision tombstone, so older upserts cannot resurrect a
departed session. Concurrent tabs never supersede one another on the wire; grouping happens only in
the product UI. Presence is best-effort and loss never blocks durable acknowledgements, recovery,
snapshots, compaction, or editing.

**Likely files**

- `apps/web/src/transport.ts`, `board-store.ts`, `board-session.ts`, `workspace/status/*`,
  `workspace/presence/*`
- `packages/protocol` and `apps/api` only in a separately reviewed presence contract

**Tests**

- Four-phase feedback model, retry countdown, queued-local wording, one recovered announcement,
  stale-generation silence, presence bounds/expiry, peer isolation, reduced-motion cursor behavior,
  no raw pointer request.
- Multi-user Playwright presence, reconnect, recovery, and revocation flows.

**Verification**

Protocol/API/web tests as affected, deterministic failure tests for presence lifecycle, Playwright
multi-user flows, performance trace, typecheck/lint/build, Prettier, diff check.

**Commit**

`feat(web): expose collaboration synchronization states`

**Explicit exclusions**

Using presence as membership, durable cursor history, exactly-once language, comments, voice/video,
or silent fallback around recovery failure.

**Stop condition**

Presence can affect durable correctness, cursor traffic is unbounded, or UI claims saved/delivered at
the wrong lifecycle boundary.

## M3.6 — Command palette, onboarding, share, and history surfaces

**Preconditions**

- M3.3–M3.5 accepted.
- Product approves which backend-missing flows remain informational versus receive separate API work.

**User-visible invariant**

Every exposed command is searchable, keyboard-safe, authorized, and truthful about whether the
underlying capability exists.

**Scope**

- Add one command registry used by shortcuts, menus, and palette.
- Add contextual onboarding and shortcut help.
- Add copy-link sharing using the current authorized board URL.
- Add synchronization/recovery details from existing client evidence.
- Add member list/invite/remove or version browse/restore only after strict authorized APIs and
  durable restore semantics are separately accepted.

**Likely files**

- `apps/web/src/commands/*`, `workspace/palette/*`, `workspace/onboarding/*`,
  `workspace/share/*`, `workspace/history/*`
- Protocol/database/API only in separately scoped prerequisites

**Tests**

- Registry uniqueness, enable/disable reasons, shortcut/input collision, palette focus, copy-link
  privacy, unsupported-feature absence, authorization and terminal-state fencing.
- If evolved: member/version API authorization, ordering, recovery, and Playwright flows.

**Verification**

Affected workspace tests, API/integration only when contracts change, Playwright keyboard/share
flows, typecheck/lint/build, Prettier, diff check.

**Commit**

`feat(web): add command and guidance surfaces`

**Explicit exclusions**

Fake member roster, inferred sharing permissions, operational recovery mutation, local-only version
restore, production OAuth, and billing.

**Stop condition**

A surface must fabricate missing server data or a palette command can bypass current readiness,
selection, authorization, or terminal state.

## M3.7 — Accessibility, responsive behavior, and themes

**Preconditions**

- Product surfaces stable through M3.6.
- Tablet, phone, and dark-theme policies approved.

**User-visible invariant**

Application chrome is fully keyboard operable and understandable without color or motion; approved
desktop/tablet/phone modes remain usable at zoom and high contrast.

**Scope**

- Complete keyboard navigation, focus order, live regions, object-list alternative, labels, and
  contrast remediation.
- Implement reduced motion and approved theme switch/system behavior.
- Implement desktop-first responsive editor, tablet policy, and phone view-only or editing policy.
- Validate 200% zoom, high contrast, touch targets, and orientation changes.

**Likely files**

- All web product surfaces, token/theme files, accessibility utilities, Playwright configuration

**Tests**

- Keyboard-only flows, focus visibility/traps, status announcements, automated accessibility scan,
  reduced motion, light/dark, high contrast, desktop/tablet/mobile viewports, zoom, touch targets.

**Verification**

Web tests/typecheck/lint/build, Chromium accessibility and responsive Playwright flows, manual
screen-reader checklist, Prettier, diff check.

**Commit**

`feat(web): complete accessible responsive themes`

**Explicit exclusions**

Unapproved phone editing, native applications, unsupported browsers, and visual changes that reduce
contrast to preserve branding.

**Stop condition**

Any critical flow lacks keyboard access, AA contrast, visible focus, reduced-motion behavior, or an
honest small-screen policy.

## M3.8 — Performance, visual regression, and final acceptance

**Preconditions**

- M3.1–M3.7 complete.
- Reference hardware/browser/DPR and screenshot review ownership approved.

**User-visible invariant**

The premium experience remains visually stable, responsive, leak-free, and correct through
collaboration, reconnect, recovery, revocation, theme, and viewport changes.

**Scope**

- Add deterministic screenshot baselines and review policy, using an approved minimal tool.
- Profile React/Konva at 100/500/1,000 object tiers and remote-presence load.
- Enforce bundle, route-load, long-task, input-latency, request-rate, and cleanup budgets.
- Run Chromium, Firefox, and WebKit final flows.
- Complete M3 claim/privacy/accessibility review and document measured limitations.

**Likely files**

- `tests/playwright/*`, visual fixtures/baselines, performance harness, CI workflow if approved,
  benchmark/design status documentation

**Tests**

- Landing/entry/workspace golden states, selected/reconnecting/recovered/revoked/blocked variants,
  reduced motion, themes, responsive viewports, multi-user collaboration, repeated session
  replacement, heap/listener cleanup, object-tier traces.

**Verification**

Full root format/lint/typecheck/test/build, integration/failure/Playwright/browser matrix as affected,
visual and performance gates, artifact privacy check, and clean Git diff.

**Commit**

`test(web): accept premium product experience`

**Explicit exclusions**

Production deployment, new capacity claims, k6 reruns, performance optimization without a reproduced
budget breach, and provider analytics.

**Stop condition**

Any M2 invariant regresses, a required browser/visual/accessibility/performance gate fails, cleanup
leaks, or M3 documentation makes an unsupported durability/capacity claim.
