# ADR 003: Board-local sequencing

Status: accepted

## Decision

Sequence numbers are scoped to a board. Assignment occurs while holding `pg_advisory_xact_lock` on a
stable 64-bit value derived from the board UUID, in the same transaction as the log, projection, and
outbox writes. `(board_id, op_id)` is unique.

## Consequences

Busy boards do not contend on a global counter, sequence assignment is monotonic per board, and retry
idempotency is enforceable at storage level. A board transaction is serialized, so command handlers
must remain short.
