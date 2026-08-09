ALTER TABLE outbox_events
  DROP CONSTRAINT outbox_payload_identity_check,
  DROP CONSTRAINT outbox_event_payload_check;

ALTER TABLE outbox_events
  ADD CONSTRAINT outbox_payload_identity_check CHECK ((
    jsonb_typeof(payload) = 'object'
    AND payload ?& ARRAY[
      'schemaVersion',
      'eventId',
      'boardId',
      'deliverySeq',
      'eventType',
      'occurredAt',
      'payload'
    ]::text[]
    AND jsonb_typeof(payload->'schemaVersion') = 'number'
    AND jsonb_typeof(payload->'eventId') = 'string'
    AND jsonb_typeof(payload->'boardId') = 'string'
    AND jsonb_typeof(payload->'deliverySeq') = 'number'
    AND jsonb_typeof(payload->'eventType') = 'string'
    AND jsonb_typeof(payload->'occurredAt') = 'string'
    AND jsonb_typeof(payload->'payload') = 'object'
    AND payload->>'schemaVersion' = schema_version::text
    AND payload->>'eventId' = id::text
    AND payload->>'boardId' = board_id::text
    AND payload->>'deliverySeq' = delivery_seq::text
    AND payload->>'eventType' = event_type
    AND payload->>'occurredAt' ~
      '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]+)?(Z|[+-][0-9]{2}:[0-9]{2})$'
  ) IS TRUE),
  ADD CONSTRAINT outbox_event_payload_check CHECK ((
    CASE event_type
      WHEN 'operation.committed' THEN
        jsonb_typeof(payload->'payload'->'operation') = 'object'
        AND payload->'payload'->'operation' ?& ARRAY['boardId', 'seq']::text[]
        AND jsonb_typeof(payload->'payload'->'operation'->'boardId') = 'string'
        AND jsonb_typeof(payload->'payload'->'operation'->'seq') = 'number'
        AND payload->'payload'->'operation'->>'boardId' = board_id::text
        AND payload->'payload'->'operation'->>'seq' = canvas_seq::text
      WHEN 'board.membership.revoked' THEN
        payload->'payload' ?& ARRAY['revokedUserId', 'initiatedByUserId']::text[]
        AND jsonb_typeof(payload->'payload'->'revokedUserId') = 'string'
        AND jsonb_typeof(payload->'payload'->'initiatedByUserId') = 'string'
        AND payload->'payload'->>'revokedUserId' ~*
          '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        AND payload->'payload'->>'initiatedByUserId' ~*
          '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      ELSE FALSE
    END
  ) IS TRUE);
