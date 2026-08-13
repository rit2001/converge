# M2.8 isolated k6 smoke result

This is a correctness smoke run, not a capacity benchmark.

- Topology: one production-composed distributed API, one production-composed worker, one disposable PostgreSQL database migrated through 0009, and one test-owned Redis delivery stream.
- Profile: smoke, 2 VUs for 30 seconds, using the real Engine.IO v4 / Socket.IO v4 collaboration protocol.
- Thresholds: all configured smoke thresholds passed; 108 distinct commands were acknowledged and received their matching logical deliveries; 163 total valid live events were observed, including peer activity.
- At-least-once evidence: 31 valid transport duplicates were observed and 31 were suppressed before logical application or command completion. This is informational deduplication evidence, not a failure.
- Correctness: protocol failures, sequence gaps, unexpected disconnects, and logical reapplications were zero. 108 authoritative logical reducer applications, 108 distinct durable operation rows, 108 distinct outbox rows, both heads, publication, and API consumer handling converged.
- Cleanup: API, worker, listeners, database, Redis key, timers, and benchmark process were released; PostgreSQL and Redis were restored to their initial stopped state.
- Pre-acceptance corrections: zero-delay command scheduling, iteration identity reuse, authoritative snapshot ownership, and catch-up/live overlap reconciliation were corrected; the lifecycle now requires command, acknowledgement, and matching delivery evidence before normal closure.

No production, baseline, scale-step, capacity, or 10,000-user claim is made.
