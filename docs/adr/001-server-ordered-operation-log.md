# ADR 001: Server-ordered operation log

Status: accepted

## Decision

Every durable board mutation is a command validated by the API and committed into a PostgreSQL
operation log. The server assigns the definitive board-local sequence. Clients render optimistically
but reconcile to committed order.

## Consequences

Ordering, authorization, idempotency, projections, and recovery are explicit and testable. Clients
cannot commit offline until reconnecting. PostgreSQL remains the recoverable source; Socket.IO and
future Redis fan-out are delivery mechanisms, never authority.
