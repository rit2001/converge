# Milestone 3 — premium product experience

Milestone 3 is complete on the accepted pinned-Chromium reference. This release gate validates the
current product experience; it is not a deployment, production-capacity, browser-certification, or
performance-SLA claim.

## Accepted product capabilities

- A tokenized landing and Studio experience with System, Light, and Dark device-local preferences
  applied before hydration.
- Authoritative object creation, selection, drag, resize, delete, and shared rotation, plus local-only
  hide and lock controls that remain recoverable through Layers.
- Deterministic positional snapping and non-interactive alignment/selection/rotation overlays.
- A semantic Layers object navigator with generated non-ID descriptions, keyboard focus/navigation,
  selection synchronization, and bounded completed-action announcements.
- Truthful synchronization status and sanitized recovery details derived from authoritative client
  lifecycle and pending-command evidence.
- Ephemeral collaborator grouping, availability, roster, and non-interactive world-coordinate cursors,
  isolated from durable editing and recovery readiness.
- A typed command palette, centralized guarded shortcuts, first-run guidance, Studio Help, and
  keyboard-only viewport-center creation, movement, resize, rotation, lock recovery, and deletion.
- Canonical authenticated board deep links and an access-preserving Share dialog. Shared URLs never
  grant access.
- Full desktop editing, compact tablet editing/touch navigation, and a truthful phone-sized view-only
  capability fence that preserves synchronization, pan/zoom, presence, and semantic Layers access.

## Deterministic visual evidence

Ten maintainable baselines cover:

- landing in Light and Dark at 1440×900;
- synchronized Studio onboarding, selected Layers/rotation controls, command palette, Help, and Share
  at 1440×900;
- synchronization details at 1024×768;
- Dark phone view-only Layers at 390×844;
- the real Light collaborator roster in the A/A2/B dual-replica topology.

The visual harness disables animations and caret instability. It masks only the Share dialog’s
canonical URL value because the required board UUID is deliberately dynamic. Immediate baseline
comparison passed. Manual inspection found no clipping, unintended overlap, horizontal overflow,
broken stacking, canvas obstruction, or disappearing theme controls. Screenshot tolerance is bounded
to a 0.5% pixel ratio with a 0.15 per-pixel threshold on pinned Chromium.

## Accessibility and responsive evidence

- Axe WCAG A/AA tags, including supported WCAG 2.2 tags, reported zero critical or serious violations
  across Light and Dark landing, empty Studio, Layers/object navigator, command palette, Help, and Share
  surfaces.
- Keyboard browser workflows passed for creation, Layers selection/navigation, canvas focus, 1/10-unit
  movement and resize, lock fencing/recovery, delete, palette execution, Help, Share, Escape precedence,
  and focus restoration.
- The semantic “Board objects” Layers region remains the Canvas alternative; IDs are absent from
  visible and accessible object labels.
- Existing forced-colors and reduced-motion browser assertions passed. Phone view-only mutation fencing,
  semantic Layers inspection, and no-horizontal-overflow evidence passed.

Automated axe and pinned-browser evidence supplement, but do not constitute, full manual accessibility
or assistive-technology certification.

## Synchronization and presence evidence

The accepted M3.5 topology uses two independently production-composed APIs, two independently compiled
web origins, a disposable migrated PostgreSQL database, real Redis presence, A/A2/B authenticated
browser sessions, separate Redis command/publisher/subscriber resources, and no Socket.IO Redis adapter
or in-process cross-API presence bus.

The final browser gate passed self-specific session identification, multi-tab grouping, cross-replica
snapshot/upsert/cursor routing, room isolation, effective leave, presence-only interruption, automatic
generation-fenced recovery, fresh snapshots, and continued PostgreSQL-backed editing while presence was
unavailable. Presence availability never changed synchronization readiness language.

## Build and bundle evidence

The frozen dependency check, repository format, all workspace lint/typecheck tasks, complete web and
protocol suites, broader unit suite, production build, API presence regressions, ordinary Playwright
suite, and dual-replica Playwright suite passed.

The independently validated production-manifest checker recorded:

| Asset                      | Gzip bytes |  Budget |
| -------------------------- | ---------: | ------: |
| Landing initial JavaScript |    106,857 | 184,320 |
| Studio initial JavaScript  |    163,266 | 460,800 |
| Initial route CSS          |      8,511 |  81,920 |

The checker counts shared/route assets once, rejects malformed or private evidence, and verifies that
known Studio-only modules are absent from the landing bundle.

## Performance limitation

M3.8A2 is permanently closed as an unaccepted measurement attempt. Its diagnostic rendered exact 100,
500, and 1,000-object Layers counts without page errors, hydration errors, or horizontal overflow, but
selection-timing harness defects invalidated latency acceptance. No frontend-latency, frame-rate,
1,000-object performance, concurrent-user, production-capacity, or SLA guarantee is claimed. No further
M3 benchmark execution was performed.

## Deferred scope

The following remain future work and are not implied by M3:

- production authentication/account UI;
- invitations, member listing, and role administration;
- user-facing version listing, preview, or forward-only restore;
- undo/redo, multi-select, grouping, durable reorder/rename, comments, or chat;
- production deployment and certified browser/device compatibility;
- certified accessibility and production-capacity/load evidence.

No release-blocking P0/P1 product defect remained. The release gate corrected only two stale Playwright
assertions that still referenced the superseded pre-M3.7C1 phone notice label. Non-blocking visual polish,
additional browser coverage, authentication UI, and deployment validation remain deferred rather than
expanding this acceptance slice.
