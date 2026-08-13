ALTER TABLE boards
  ADD COLUMN operation_recovery_floor bigint NOT NULL DEFAULT 0,
  ADD COLUMN delivery_recovery_floor bigint NOT NULL DEFAULT 0;

ALTER TABLE boards
  ADD CONSTRAINT boards_operation_recovery_floor_check CHECK (
    operation_recovery_floor BETWEEN 0 AND 9007199254740991
    AND operation_recovery_floor <= last_seq
  ),
  ADD CONSTRAINT boards_delivery_recovery_floor_check CHECK (
    delivery_recovery_floor BETWEEN 0 AND 9007199254740991
    AND delivery_recovery_floor <= last_delivery_seq
  );

CREATE OR REPLACE FUNCTION converge_reject_recovery_floor_regression() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF NEW.operation_recovery_floor < OLD.operation_recovery_floor
    OR NEW.delivery_recovery_floor < OLD.delivery_recovery_floor
  THEN
    RAISE EXCEPTION 'board recovery floors cannot move backward' USING ERRCODE = '55000';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER boards_recovery_floors_forward_only
BEFORE UPDATE OF operation_recovery_floor, delivery_recovery_floor ON boards
FOR EACH ROW EXECUTE FUNCTION converge_reject_recovery_floor_regression();

CREATE OR REPLACE FUNCTION converge_operation_receipt_is_valid(
  candidate_command jsonb,
  candidate_result jsonb,
  expected_board_id uuid,
  expected_operation_id uuid,
  expected_canvas_seq bigint,
  expected_delivery_seq bigint,
  expected_event_id uuid,
  expected_committed_at timestamptz,
  expected_hash_schema_version integer,
  expected_command_hash text
) RETURNS boolean
LANGUAGE plpgsql
IMMUTABLE
STRICT
AS $$
DECLARE
  operation_result jsonb;
  command_payload jsonb;
  command_type text;
  payload_entry record;
BEGIN
  IF jsonb_typeof(candidate_command) <> 'object'
    OR NOT candidate_command ?& ARRAY[
      'schemaVersion', 'opId', 'boardId', 'clientId', 'baseSeq', 'type', 'targetId',
      'payload', 'clientTimestamp'
    ]::text[]
    OR candidate_command - ARRAY[
      'schemaVersion', 'opId', 'boardId', 'clientId', 'baseSeq', 'type', 'targetId',
      'payload', 'clientTimestamp'
    ]::text[] <> '{}'::jsonb
    OR jsonb_typeof(candidate_command->'schemaVersion') <> 'number'
    OR jsonb_typeof(candidate_command->'opId') <> 'string'
    OR jsonb_typeof(candidate_command->'boardId') <> 'string'
    OR jsonb_typeof(candidate_command->'clientId') <> 'string'
    OR jsonb_typeof(candidate_command->'baseSeq') <> 'number'
    OR jsonb_typeof(candidate_command->'type') <> 'string'
    OR jsonb_typeof(candidate_command->'targetId') <> 'string'
    OR jsonb_typeof(candidate_command->'payload') <> 'object'
    OR jsonb_typeof(candidate_command->'clientTimestamp') <> 'string'
    OR candidate_command->>'schemaVersion' <> '1'
    OR candidate_command->>'baseSeq' !~ '^(0|[1-9][0-9]*)$'
    OR (candidate_command->>'baseSeq')::numeric > 9007199254740991
    OR (candidate_command->>'opId')::uuid <> expected_operation_id
    OR (candidate_command->>'boardId')::uuid <> expected_board_id
    OR (candidate_command->>'clientId')::uuid::text <> lower(candidate_command->>'clientId')
    OR (candidate_command->>'targetId')::uuid::text <> lower(candidate_command->>'targetId')
    OR candidate_command->>'clientTimestamp' !~
      '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]+)?(Z|[+-][0-9]{2}:[0-9]{2})$'
    OR expected_canvas_seq NOT BETWEEN 1 AND 9007199254740991
    OR expected_delivery_seq NOT BETWEEN expected_canvas_seq AND 9007199254740991
    OR expected_hash_schema_version <> 1
    OR expected_command_hash <> encode(
      sha256(convert_to('converge.operation-command.v1:' || candidate_command::text, 'UTF8')),
      'hex'
    )
  THEN
    RETURN false;
  END IF;

  PERFORM (candidate_command->>'clientTimestamp')::timestamptz;
  command_type := candidate_command->>'type';
  command_payload := candidate_command->'payload';

  IF command_type = 'object.create' THEN
    IF NOT command_payload ?& ARRAY[
      'id', 'kind', 'x', 'y', 'width', 'height', 'rotation', 'fill', 'text'
    ]::text[]
      OR command_payload - ARRAY[
        'id', 'kind', 'x', 'y', 'width', 'height', 'rotation', 'fill', 'text'
      ]::text[] <> '{}'::jsonb
      OR jsonb_typeof(command_payload->'id') <> 'string'
      OR jsonb_typeof(command_payload->'kind') <> 'string'
      OR jsonb_typeof(command_payload->'x') <> 'number'
      OR jsonb_typeof(command_payload->'y') <> 'number'
      OR jsonb_typeof(command_payload->'width') <> 'number'
      OR jsonb_typeof(command_payload->'height') <> 'number'
      OR jsonb_typeof(command_payload->'rotation') <> 'number'
      OR jsonb_typeof(command_payload->'fill') <> 'string'
      OR jsonb_typeof(command_payload->'text') <> 'string'
      OR (command_payload->>'id')::uuid <> (candidate_command->>'targetId')::uuid
      OR command_payload->>'kind' NOT IN ('rectangle', 'sticky')
      OR (command_payload->>'x')::numeric NOT BETWEEN -1000000 AND 1000000
      OR (command_payload->>'y')::numeric NOT BETWEEN -1000000 AND 1000000
      OR (command_payload->>'width')::numeric NOT BETWEEN 8 AND 100000
      OR (command_payload->>'height')::numeric NOT BETWEEN 8 AND 100000
      OR (command_payload->>'rotation')::numeric NOT BETWEEN -1000000 AND 1000000
      OR command_payload->>'fill' !~ '^#[0-9a-fA-F]{6}$'
      OR char_length(command_payload->>'text') > 10000
      OR (command_payload->>'kind' = 'rectangle' AND command_payload->>'text' <> '')
    THEN
      RETURN false;
    END IF;
  ELSIF command_type = 'object.update' THEN
    IF command_payload = '{}'::jsonb
      OR command_payload - ARRAY['fill', 'text']::text[] <> '{}'::jsonb
      OR (command_payload ? 'fill' AND (
        jsonb_typeof(command_payload->'fill') <> 'string'
        OR command_payload->>'fill' !~ '^#[0-9a-fA-F]{6}$'
      ))
      OR (command_payload ? 'text' AND (
        jsonb_typeof(command_payload->'text') <> 'string'
        OR char_length(command_payload->>'text') > 10000
      ))
    THEN
      RETURN false;
    END IF;
  ELSIF command_type = 'object.transform' THEN
    IF command_payload = '{}'::jsonb
      OR command_payload - ARRAY['x', 'y', 'width', 'height', 'rotation']::text[] <> '{}'::jsonb
    THEN
      RETURN false;
    END IF;
    FOR payload_entry IN SELECT key, value FROM jsonb_each(command_payload)
    LOOP
      IF jsonb_typeof(payload_entry.value) <> 'number'
        OR (payload_entry.value #>> '{}')::numeric NOT BETWEEN
          (CASE WHEN payload_entry.key IN ('width', 'height') THEN 8 ELSE -1000000 END)
          AND (CASE WHEN payload_entry.key IN ('width', 'height') THEN 100000 ELSE 1000000 END)
      THEN
        RETURN false;
      END IF;
    END LOOP;
  ELSIF command_type = 'object.delete' THEN
    IF command_payload <> '{}'::jsonb THEN
      RETURN false;
    END IF;
  ELSE
    RETURN false;
  END IF;

  IF jsonb_typeof(candidate_result) <> 'object'
    OR NOT candidate_result ?& ARRAY[
      'schemaVersion', 'eventId', 'boardId', 'deliverySeq', 'eventType', 'occurredAt', 'payload'
    ]::text[]
    OR candidate_result - ARRAY[
      'schemaVersion', 'eventId', 'boardId', 'deliverySeq', 'eventType', 'occurredAt', 'payload'
    ]::text[] <> '{}'::jsonb
    OR jsonb_typeof(candidate_result->'schemaVersion') <> 'number'
    OR jsonb_typeof(candidate_result->'eventId') <> 'string'
    OR jsonb_typeof(candidate_result->'boardId') <> 'string'
    OR jsonb_typeof(candidate_result->'deliverySeq') <> 'number'
    OR jsonb_typeof(candidate_result->'eventType') <> 'string'
    OR jsonb_typeof(candidate_result->'occurredAt') <> 'string'
    OR jsonb_typeof(candidate_result->'payload') <> 'object'
    OR candidate_result->>'schemaVersion' <> '1'
    OR (candidate_result->>'eventId')::uuid <> expected_event_id
    OR (candidate_result->>'boardId')::uuid <> expected_board_id
    OR candidate_result->>'deliverySeq' <> expected_delivery_seq::text
    OR candidate_result->>'eventType' <> 'operation.committed'
    OR candidate_result->>'occurredAt' !~
      '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]+)?(Z|[+-][0-9]{2}:[0-9]{2})$'
    OR (candidate_result->>'occurredAt')::timestamptz <> expected_committed_at
    OR (candidate_result->'payload') - ARRAY['operation']::text[] <> '{}'::jsonb
    OR jsonb_typeof(candidate_result->'payload'->'operation') <> 'object'
  THEN
    RETURN false;
  END IF;

  operation_result := candidate_result->'payload'->'operation';
  IF operation_result - ARRAY['seq', 'committedAt']::text[] <> candidate_command
    OR jsonb_typeof(operation_result->'seq') <> 'number'
    OR operation_result->>'seq' <> expected_canvas_seq::text
    OR jsonb_typeof(operation_result->'committedAt') <> 'string'
    OR operation_result->>'committedAt' <> candidate_result->>'occurredAt'
  THEN
    RETURN false;
  END IF;

  RETURN true;
EXCEPTION WHEN others THEN
  RETURN false;
END;
$$;

CREATE TABLE board_operation_receipts (
  board_id uuid NOT NULL REFERENCES boards(id) ON DELETE CASCADE,
  operation_id uuid NOT NULL,
  actor_id uuid NOT NULL,
  command jsonb NOT NULL,
  command_hash text NOT NULL CHECK (command_hash ~ '^[0-9a-f]{64}$'),
  hash_schema_version integer NOT NULL CHECK (hash_schema_version = 1),
  canvas_seq bigint NOT NULL CHECK (canvas_seq BETWEEN 1 AND 9007199254740991),
  delivery_seq bigint NOT NULL CHECK (
    delivery_seq BETWEEN canvas_seq AND 9007199254740991
  ),
  event_id uuid NOT NULL,
  committed_at timestamptz NOT NULL,
  result jsonb NOT NULL,
  PRIMARY KEY (board_id, operation_id),
  CONSTRAINT board_operation_receipts_board_canvas_seq_uq UNIQUE (board_id, canvas_seq),
  CONSTRAINT board_operation_receipts_board_delivery_seq_uq UNIQUE (board_id, delivery_seq),
  CONSTRAINT board_operation_receipts_event_id_uq UNIQUE (event_id),
  CONSTRAINT board_operation_receipts_consistency_check CHECK ((
    converge_operation_receipt_is_valid(
      command,
      result,
      board_id,
      operation_id,
      canvas_seq,
      delivery_seq,
      event_id,
      committed_at,
      hash_schema_version,
      command_hash
    )
  ) IS TRUE)
);

INSERT INTO board_operation_receipts(
  board_id, operation_id, actor_id, command, command_hash, hash_schema_version,
  canvas_seq, delivery_seq, event_id, committed_at, result
)
SELECT
  operation.board_id,
  operation.op_id,
  operation.user_id,
  operation.command,
  encode(
    sha256(convert_to('converge.operation-command.v1:' || operation.command::text, 'UTF8')),
    'hex'
  ),
  1,
  operation.seq,
  operation.delivery_seq,
  operation.event_id,
  operation.committed_at,
  event.payload
FROM board_operations AS operation
JOIN outbox_events AS event
  ON event.id = operation.event_id
 AND event.board_id = operation.board_id
 AND event.delivery_seq = operation.delivery_seq
 AND event.canvas_seq = operation.seq
 AND event.event_type = 'operation.committed'
ON CONFLICT (board_id, operation_id) DO NOTHING;

DO $$
DECLARE
  operation_count bigint;
  receipt_count bigint;
BEGIN
  SELECT count(*) INTO operation_count FROM board_operations;
  SELECT count(*) INTO receipt_count FROM board_operation_receipts;

  IF operation_count <> receipt_count
    OR EXISTS (
      SELECT 1
      FROM board_operations AS operation
      FULL JOIN board_operation_receipts AS receipt
        ON receipt.board_id = operation.board_id
       AND receipt.operation_id = operation.op_id
      WHERE operation.board_id IS NULL
        OR receipt.board_id IS NULL
        OR receipt.actor_id IS DISTINCT FROM operation.user_id
        OR receipt.command IS DISTINCT FROM operation.command
        OR receipt.canvas_seq IS DISTINCT FROM operation.seq
        OR receipt.delivery_seq IS DISTINCT FROM operation.delivery_seq
        OR receipt.event_id IS DISTINCT FROM operation.event_id
        OR receipt.committed_at IS DISTINCT FROM operation.committed_at
    )
  THEN
    RAISE EXCEPTION 'M2.7 receipt backfill does not have one-to-one operation identity parity';
  END IF;
END;
$$;

CREATE OR REPLACE FUNCTION converge_reject_operation_receipt_mutation() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  RAISE EXCEPTION 'board operation receipts are immutable' USING ERRCODE = '55000';
END;
$$;

CREATE TRIGGER board_operation_receipts_immutable
BEFORE UPDATE ON board_operation_receipts
FOR EACH ROW EXECUTE FUNCTION converge_reject_operation_receipt_mutation();

COMMENT ON TABLE board_operation_receipts IS
  'Board-lifetime exact-replay evidence; never eligible for operation/outbox compaction';
