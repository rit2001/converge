# M2 k6 collaboration workload methodology

This document defines the reproducible workload contract used by the isolated correctness smoke and
one controlled local baseline. The recorded baseline is evidence for that exact topology and profile;
it is not a throughput-capacity, scalability, production, or 10,000-client claim.

## Protocol compatibility and workload model

Converge uses Socket.IO 4.8.1 over Engine.IO v4. Vanilla k6 can faithfully exercise the current
protocol with its supported `k6/ws` WebSocket API; no extension is required. The workload implements
only the root-namespace frames used by Converge: strict Engine.IO open, Engine.IO ping/pong, Socket.IO
namespace authentication, event packets with acknowledgement IDs, acknowledgement packets, live
events, connection errors, and disconnects. Unknown, malformed, oversized, or uncorrelated packets
fail the iteration.

Each smoke or baseline virtual user:

1. opens a WebSocket-only Engine.IO v4 connection and authenticates the Socket.IO namespace with the
   configured opaque token;
2. joins one existing authorized board with fresh connection-local synchronization state;
3. in smoke, submits create-only commands; in baseline, creates one stable object per VU and then
   submits `object.update` mutations with unique command/operation identities;
4. validates the committed acknowledgement against its local board and operation evidence;
5. accepts ordered live delivery, detects duplicate logical operations without applying them twice,
   and rejects silent sequence gaps or regressions;
6. uses the bounded authenticated operation-range endpoint when live delivery does not arrive before
   the acknowledgement deadline, then strictly validates contiguous authoritative catch-up evidence;
7. terminates participation on access revocation; and
8. sends a Socket.IO disconnect and closes the WebSocket.

The exploratory `scale-step` profile uses different lifecycle ownership: each VU loads one snapshot,
joins once, keeps one connection open, creates one stable object if needed, and submits sequential
`object.update` mutations every 250 ms. It closes before ramp-down and remains parked inside the
same invocation, preventing reconnect and snapshot amplification. This persistent profile and its
streaming harness remain reproducible diagnostic tooling, but the recorded local scale observation
failed its session-correctness gates and is not accepted benchmark evidence.

The workload retains only a bounded 256-operation deduplication window and current sequence evidence,
not the board projection. Publication and delivery remain at least once: duplicate detection is a
correctness check, not an exactly-once claim.

A transport duplicate is valid at-least-once evidence observed through catch-up/live overlap,
snapshot-covered delayed delivery, or redelivery when stable identity and canonical logical content
agree. `converge_duplicate_events` counts these observations after they are suppressed; a nonzero
value is informational and is not itself a capacity or reliability failure. A logical reapplication
is different: applying one logical operation twice, completing one command twice, accepting
conflicting duplicate evidence, or creating extra durable operation/outbox state is a correctness
failure. Smoke acceptance requires zero logical reapplications, proven by bounded lifecycle
accounting and equality of distinct commands, durable operation rows, outbox events, canvas/delivery
heads, publications, and API consumer progress—not by requiring zero duplicate arrivals. Baseline
projection cardinality instead plateaus at the number of initialized VUs while durable history grows.

## Safe profiles

These are configuration presets, not measured claims:

| Profile      | Preset                                                                                    | Use                                                        |
| ------------ | ----------------------------------------------------------------------------------------- | ---------------------------------------------------------- |
| `smoke`      | 2 VUs, 30 seconds, 2 commands/client, 1-second command interval                           | Protocol and topology validation only                      |
| `baseline`   | 10 VUs, 2 minutes, 10 commands/client                                                     | Explicit opt-in controlled baseline; never automatic in CI |
| `scale-step` | 15s ramp/30s hold at 10, 30s ramp/45s hold at 50, 30s ramp/60s hold at 100, 15s ramp down | Explicit opt-in exploratory local scale observation        |

There is no 10,000-user local default. Tests at 1,000 or 10,000 connected clients require provisioned
distributed infrastructure, isolated data, capacity planning, and a separately reviewed execution
plan. They are not current capability claims.

## Configuration and target safeguards

Every stateful invocation requires:

- `CONVERGE_BASE_URL`
- `CONVERGE_SOCKET_URL`
- `CONVERGE_BOARD_ID`
- `CONVERGE_AUTH_TOKEN`

The board must be an isolated benchmark-owned fixture that already exists and authorizes the token;
the workload must never target production data. This contract slice does not execute the workload or
create any data.

The current bounded initial catch-up permits at most 1,000 pre-existing operations, so benchmark
fixtures must record and remain within that starting-history limit.

The token is carried only in Socket.IO auth and the bounded catch-up Authorization header. It is never
logged or used as a metric tag or diagnostic value.

Optional bounded settings are `CONVERGE_PROFILE`, `CONVERGE_VUS`, `CONVERGE_DURATION`,
`CONVERGE_COMMANDS_PER_CLIENT`, `CONVERGE_COMMAND_INTERVAL_MS`, `CONVERGE_MAX_PACKET_BYTES`,
`CONVERGE_CONNECT_TIMEOUT_MS`, and `CONVERGE_ACK_TIMEOUT_MS`. Invalid, zero, negative, excessive,
credential-bearing, query-bearing, or malformed values fail during module initialization. Overrides
remain capped at 100 VUs, 10 minutes, 100 commands/client, 1 MiB/packet, and 60 seconds for intervals
and timeouts.

Loopback targets are allowed. Any non-loopback HTTP or Socket target additionally requires exact
`CONVERGE_ALLOW_REMOTE_TARGET=true`; there is no implicit production target. The repository command
does not start Compose, PostgreSQL, Redis, API, worker, or any other service.

`pnpm benchmark:k6:check` runs pure protocol/configuration tests and lint without k6 or services.
`pnpm benchmark:k6:smoke` requires all stateful variables and an installed trusted `k6` executable;
missing k6 fails explicitly. Smoke is intentionally absent from CI in this slice.

## Metrics and correctness gates

Custom metric names are fixed:

- trends: `converge_socket_connect_duration`, `converge_board_join_ack_duration`,
  `converge_command_ack_duration`, `converge_live_delivery_duration`;
- rates: `converge_iteration_failures`, `converge_protocol_failures`; and
- counters: `converge_duplicate_events`, `converge_sequence_gaps`,
  `converge_commands_acknowledged`, `converge_live_events_received`,
  `converge_snapshot_requests`, `converge_range_requests`,
  `converge_scale_sessions_started`, `converge_scale_sessions_initialized`,
  `converge_scale_sessions_completed`, `converge_scale_second_invocations`,
  `converge_scale_unexpected_disconnects`, and `converge_scale_session_failures`.

Only `profile`, `operation_type`, `outcome`, and a fixed allowlisted failure `reason` may be attached
by the workload. Board, principal,
operation, event, socket, Redis-entry and sequence identifiers, URLs, and error messages are forbidden
as tags. k6 system tags are disabled so built-in WebSocket and catch-up HTTP metrics cannot introduce
URL or per-client cardinality.

Initial pre-benchmark thresholds are:

- iteration failure rate below 1%;
- protocol failure rate exactly zero;
- sequence gaps exactly zero;
- command acknowledgement p99 below 2 seconds for smoke/baseline;
- socket connection p99 below 5 seconds; and
- board join acknowledgement p99 below 5 seconds.

These gates passed for the recorded isolated smoke and controlled local baseline. A protocol mismatch, authentication or
join failure, uncorrelated acknowledgement, invalid committed identity, malformed live event,
unresolved gap, failed catch-up, timeout, or unexpected disconnect is not counted as success.

## Execution and publication policy

A future published run must record the exact Git commit; k6, Node, Socket.IO, Engine.IO, PostgreSQL,
and Redis versions; host/cloud model; CPU model/count; memory; storage; operating system/kernel;
container/runtime versions; network placement and latency; API/worker replica counts and limits;
database and Redis topology; delivery mode; profile and sanitized overrides; board starting sequence;
and whether any dependency was shared. Tokens, URLs with hosts, identifiers, commands, payloads, and
raw frames must be excluded from artifacts.

The recorded baseline measured its complete two-minute preset and did not include a separately
excluded warm-up window; that is an explicit limitation of this single run. Any future comparative
or repeated study must define a separately labeled warm-up based on connection/runtime stabilization,
exclude it from the measured window, and then hold topology and workload constant. Aborted,
threshold-failing, or correctness-failing runs remain diagnostic evidence but cannot support a
capacity claim.

Accepted artifacts are versioned directories containing sanitized run metadata, k6 JSON summary
output, threshold/correctness status, aggregate evidence, and operator notes. Raw application
payloads and identifiers are never artifacts. The controlled baseline result is recorded at
`docs/benchmarks/results/m2-k6-baseline-20260813T125350228Z/`. The failed local scale executions are
documented separately as an [unaccepted observation](./m2-scale-step-observation.md); no accepted
scale result directory exists. High-concurrency refinement and any further scale execution are
deferred to a future performance milestone.
