ALTER TABLE boards
  ADD COLUMN last_delivery_seq bigint NOT NULL DEFAULT 0;

ALTER TABLE outbox_events
  RENAME COLUMN board_seq TO canvas_seq;

ALTER INDEX IF EXISTS outbox_operation_board_seq_uq
  RENAME TO outbox_operation_canvas_seq_uq;

ALTER TABLE outbox_events
  ADD COLUMN delivery_seq bigint,
  ADD COLUMN schema_version integer;

ALTER TABLE board_operations
  ADD COLUMN event_id uuid,
  ADD COLUMN delivery_seq bigint;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM outbox_events
    WHERE event_type NOT IN ('operation.committed', 'board.membership.revoked')
  ) THEN
    RAISE EXCEPTION 'M2.1 cannot backfill an unknown legacy outbox event type';
  END IF;
END
$$;

-- M1 did not persist cross-type commit order. The approved drained cutover defines a migration-only
-- order: canvas operations first by their authoritative sequence, then control events by creation
-- time and stable event ID. This is deterministic but does not claim to reconstruct live M1 order.
WITH ranked AS (
  SELECT
    id,
    row_number() OVER (
      PARTITION BY board_id
      ORDER BY
        CASE WHEN event_type = 'operation.committed' THEN 0 ELSE 1 END,
        CASE WHEN event_type = 'operation.committed' THEN canvas_seq END,
        CASE WHEN event_type <> 'operation.committed' THEN created_at END,
        id
    ) AS assigned_delivery_seq
  FROM outbox_events
)
UPDATE outbox_events AS event
SET delivery_seq = ranked.assigned_delivery_seq,
    schema_version = 1
FROM ranked
WHERE event.id = ranked.id;

-- Convert every historical M1 payload to the complete strict M2 envelope. The stable outbox ID is
-- retained as the domain event ID.
UPDATE outbox_events
SET payload = CASE event_type
  WHEN 'operation.committed' THEN jsonb_build_object(
    'schemaVersion', 1,
    'eventId', id,
    'boardId', board_id,
    'deliverySeq', delivery_seq,
    'eventType', event_type,
    'occurredAt', COALESCE(payload->'committedAt', to_jsonb(created_at)),
    'payload', jsonb_build_object('operation', payload)
  )
  WHEN 'board.membership.revoked' THEN jsonb_build_object(
    'schemaVersion', 1,
    'eventId', id,
    'boardId', board_id,
    'deliverySeq', delivery_seq,
    'eventType', event_type,
    'occurredAt', COALESCE(payload->'committedAt', to_jsonb(created_at)),
    'payload', jsonb_build_object(
      'revokedUserId', payload->'revokedUserId',
      'initiatedByUserId', payload->'initiatedByUserId'
    )
  )
END;

UPDATE board_operations AS operation
SET event_id = event.id,
    delivery_seq = event.delivery_seq
FROM outbox_events AS event
WHERE event.board_id = operation.board_id
  AND event.event_type = 'operation.committed'
  AND event.canvas_seq = operation.seq;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM board_operations
    WHERE event_id IS NULL OR delivery_seq IS NULL
  ) THEN
    RAISE EXCEPTION 'M2.1 legacy operation is missing its atomic outbox event';
  END IF;
END
$$;

UPDATE boards AS board
SET last_delivery_seq = COALESCE(
  (
    SELECT max(event.delivery_seq)
    FROM outbox_events AS event
    WHERE event.board_id = board.id
  ),
  0
);

-- There was no M1 dispatcher. During the no-writer cutover, all deterministic legacy rows become
-- historical so that starting an M2 publisher cannot emit them as newly pending work.
UPDATE outbox_events
SET published_at = COALESCE(published_at, now());

ALTER TABLE boards
  ADD CONSTRAINT boards_last_delivery_seq_check CHECK (last_delivery_seq >= 0),
  ADD CONSTRAINT boards_delivery_head_covers_canvas_check
    CHECK (last_delivery_seq >= last_seq);

ALTER TABLE board_operations
  ALTER COLUMN event_id SET NOT NULL,
  ALTER COLUMN delivery_seq SET NOT NULL,
  ADD CONSTRAINT board_operations_delivery_seq_check CHECK (delivery_seq > 0),
  ADD CONSTRAINT board_operations_event_id_uq UNIQUE (event_id),
  ADD CONSTRAINT board_operations_board_delivery_seq_uq UNIQUE (board_id, delivery_seq);

ALTER TABLE outbox_events
  ALTER COLUMN delivery_seq SET NOT NULL,
  ALTER COLUMN schema_version SET NOT NULL,
  ADD CONSTRAINT outbox_delivery_seq_check CHECK (delivery_seq > 0),
  ADD CONSTRAINT outbox_schema_version_check CHECK (schema_version = 1),
  ADD CONSTRAINT outbox_event_type_check
    CHECK (event_type IN ('operation.committed', 'board.membership.revoked')),
  ADD CONSTRAINT outbox_event_canvas_seq_check CHECK (
    (event_type = 'operation.committed' AND canvas_seq IS NOT NULL AND canvas_seq > 0)
    OR (event_type = 'board.membership.revoked' AND canvas_seq IS NULL)
  ),
  ADD CONSTRAINT outbox_payload_identity_check CHECK (
    jsonb_typeof(payload) = 'object'
    AND payload->>'schemaVersion' = schema_version::text
    AND payload->>'eventId' = id::text
    AND payload->>'boardId' = board_id::text
    AND payload->>'deliverySeq' = delivery_seq::text
    AND payload->>'eventType' = event_type
    AND payload ? 'occurredAt'
    AND jsonb_typeof(payload->'payload') = 'object'
  ),
  ADD CONSTRAINT outbox_event_payload_check CHECK (
    (
      event_type = 'operation.committed'
      AND payload->'payload'->'operation'->>'boardId' = board_id::text
      AND payload->'payload'->'operation'->>'seq' = canvas_seq::text
    )
    OR (
      event_type = 'board.membership.revoked'
      AND payload->'payload' ? 'revokedUserId'
      AND payload->'payload' ? 'initiatedByUserId'
    )
  ),
  ADD CONSTRAINT outbox_board_delivery_seq_uq UNIQUE (board_id, delivery_seq);
