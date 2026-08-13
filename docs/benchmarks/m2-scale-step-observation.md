# M2 scale-step observation

**Status: UNACCEPTED LOCAL SCALE OBSERVATION**

This document records two threshold-failing local executions for diagnostic completeness. Neither is
accepted benchmark evidence, and neither establishes a reliable capacity ceiling. Accepted M2
benchmark evidence consists only of the isolated correctness smoke and the controlled 10-VU
baseline.

## Reconnect-heavy attempt

The first attempt used short reconnecting iterations. During the 50-VU stage, repeated snapshot
initialization amplified HTTP traffic and reached Fastify's intentional 120-request-per-minute global
limit. The workload measured reconnect amplification rather than sustained connected collaboration.
Production rate limits were unchanged, the result was rejected, and no capacity conclusion was
drawn.

## Persistent-session attempt

The corrected workload modeled one persistent Socket.IO connection, snapshot initialization, and
board join per VU. Each VU owned one stable canvas object and submitted sequentially acknowledgement-
and-delivery-fenced `object.update` mutations at a fixed 250 ms interval. Completed sessions closed
gracefully and remained parked inside their original invocation rather than reconnecting.

The single-machine, single-API, single-worker schedule was:

- 15 seconds ramping to 10 VUs;
- 30 seconds holding 10 VUs;
- 30 seconds ramping to 50 VUs;
- 45 seconds holding 50 VUs;
- 30 seconds ramping to 100 VUs;
- 60 seconds holding 100 VUs; and
- 15 seconds ramping down.

The unaccepted aggregate evidence was:

- 2,844 successful command acknowledgements;
- 35,924 valid live events;
- 233 valid transport duplicates suppressed before logical application;
- 98 iteration failures and 94 protocol failures;
- zero sequence gaps;
- 2,846 durable operations, outbox rows, and valid publications;
- canvas head, delivery head, and API consumer progress all at 2,846; and
- 80 initialized projection objects.

Durable PostgreSQL, outbox, publication, and API-consumer state converged, but session correctness
thresholds failed. The 80 initialized objects are evidence from this failed run; they are not the
system's maximum, do not locate the failure at precisely 80 VUs, and do not demonstrate 100-VU
support. The original streaming parser did not recognize k6-wrapped console diagnostics, so the
exact fixed phase/reason could not be recovered. The execution was not rerun.

No reliable capacity ceiling was established. High-concurrency session refinement and any further
scale execution are deferred to a future performance milestone. These observations make no
production-capacity, horizontal-scaling, SLA, deployment, multi-replica, or 10,000-user claim.
