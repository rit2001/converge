# ADR 002: Ephemeral versus durable events

Status: accepted

## Decision

Object edits and board history are durable commands. Presence, cursor, selection, transform, stroke,
and text previews are ephemeral signals: throttled, coalesced, safe to drop, and never stored in
PostgreSQL.

## Consequences

High-frequency interaction cannot inflate history or delay durable commits. A preview disappearing is
acceptable; a committed edit disappearing is not. Milestone 1 implements the durable path; ephemeral
signals are deliberately deferred.
