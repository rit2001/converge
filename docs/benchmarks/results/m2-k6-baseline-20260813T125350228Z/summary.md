# M2.8 controlled local k6 baseline

This was one local 10-VU/2-minute baseline, not a capacity or production benchmark.

- Topology: one production-composed distributed API, one production-composed worker, one disposable PostgreSQL database migrated through 0009, and one test-owned Redis delivery stream on a single machine.
- Runtime: pinned official k6 0.57.0 using the immutable image digest recorded in environment.json.
- Workload: 130 completed iterations with 10 commands per iteration; each VU created one stable object and subsequent commands used the supported object.update contract. Projection cardinality plateaued at 10 objects while 1300 logical command attempts were acknowledged and received matching logical deliveries; 12474 valid live events were observed.
- Throughput: 1.006 completed iterations/second and 10.058 acknowledged commands/second.
- Latency milliseconds (p50/p95/p99): connection 6/74.55/97.39; join 17/54.55/104.24; command acknowledgement 24/98.1/227.03; matching live delivery 64/187.15/368.
- Thresholds and correctness: all configured thresholds passed; iteration failures, protocol failures, sequence gaps/conflicts, unexpected disconnects, and logical reapplications were zero.
- At-least-once evidence: 264 valid transport duplicates were observed and 264 were suppressed before logical application or command completion. These are informational and are not logical reapplications.
- Durable convergence: 1300 durable operations, 1300 outbox rows, 1300 valid publications, canvas head 1300, delivery head 1300, and API consumer progress 1300 converged.
- Environmental limits: this single local machine, loopback network, one API replica, one worker, one run, and no separately excluded warm-up window provide no WAN, multi-replica, Redis-durability, comparative, or statistical-confidence evidence.
- Cleanup: API, worker, listeners, database, Redis key, timers, and benchmark process were released; PostgreSQL and Redis were restored to their initial state.
- Workload correction: the initial create-only baseline reached the k6 client's 131,072-byte (128 KiB) snapshot-response guard. The production snapshot schema has no 1,000-object restriction, production limits were unchanged, and the smoke profile remains a separate create-only correctness workload.

No comparison should be inferred from the earlier smoke because the profiles differ. This result makes no maximum-user, production-capacity, horizontal-scalability, exactly-once-delivery, deployment, or 10,000-user claim.
