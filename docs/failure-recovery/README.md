# Failure recovery

Milestone 1 uses a room-first join, fixed sequence watermark, bounded operation-log catch-up, and live
event buffering before reporting `READY`. The same protocol runs for initial load and reconnect, and
duplicate catch-up/live delivery is harmless. Crash-window outbox replay, Redis fan-out, server-crash
recovery, and snapshot-tail recovery remain deferred to Milestone 2.

## M2 Redis delivery boundary

The M2 delivery parser assumes a trusted, private Redis broker and an exact authorized-writer set:
workers append validated bounded delivery entries, and APIs append only the fixed bounded
initialization sentinel. Workers measure every field name/value and the complete entry before
`XADD`; oversized rejection does not mark the PostgreSQL outbox row published. API metadata and
sentinel inspection execute inside bounded Lua projections so arbitrary first/last payload fields do
not cross into JavaScript.

Consumer envelope checks occur after node-redis has decoded `XREAD`. They still prevent handler
delivery, cursor advancement, and unbounded retained BoardState/queue accounting for malformed or
oversized decoded entries, but they are not a pre-allocation network limit. A compromised Redis
credential/server is therefore an infrastructure incident: isolate the private endpoint, stop
affected consumers, revoke and rotate credentials, identify the unexpected writer and stream-growth
window, and recover authoritative client state from PostgreSQL. Deployment must provide private
networking, no public Redis endpoint, TLS and separate least-privilege ACL credentials where
supported, a rotation procedure, and monitoring for unexpected writers, malformed entries, and
stream growth. Local Compose does not claim those controls.
