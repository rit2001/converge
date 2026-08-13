CREATE OR REPLACE FUNCTION converge_snapshot_projection_is_valid(
  candidate jsonb,
  expected_board_id uuid,
  expected_snapshot_seq bigint,
  expected_delivery_seq bigint,
  expected_schema_version integer,
  expected_object_count integer
) RETURNS boolean
LANGUAGE plpgsql
IMMUTABLE
STRICT
AS $$
DECLARE
  projected_object jsonb;
  object_value jsonb;
  field_sequences jsonb;
  field_entry record;
  object_id uuid;
  stack_order bigint;
  created_seq bigint;
  updated_seq bigint;
  deleted_seq bigint;
  previous_object_id uuid;
  previous_stack_order bigint;
  object_total integer := 0;
BEGIN
  IF jsonb_typeof(candidate) <> 'object'
    OR NOT candidate ?& ARRAY[
      'schemaVersion', 'boardId', 'boardName', 'lastSeq', 'lastDeliverySeq', 'objects'
    ]
    OR candidate - ARRAY[
      'schemaVersion', 'boardId', 'boardName', 'lastSeq', 'lastDeliverySeq', 'objects'
    ]::text[] <> '{}'::jsonb
    OR jsonb_typeof(candidate->'schemaVersion') <> 'number'
    OR jsonb_typeof(candidate->'boardId') <> 'string'
    OR jsonb_typeof(candidate->'boardName') <> 'string'
    OR jsonb_typeof(candidate->'lastSeq') <> 'number'
    OR jsonb_typeof(candidate->'lastDeliverySeq') <> 'number'
    OR jsonb_typeof(candidate->'objects') <> 'array'
    OR char_length(candidate->>'boardName') NOT BETWEEN 1 AND 120
    OR candidate->>'schemaVersion' !~ '^[1-9][0-9]*$'
    OR candidate->>'lastSeq' !~ '^(0|[1-9][0-9]*)$'
    OR candidate->>'lastDeliverySeq' !~ '^(0|[1-9][0-9]*)$'
    OR (candidate->>'schemaVersion')::numeric > 2147483647
    OR (candidate->>'lastSeq')::numeric > 9007199254740991
    OR (candidate->>'lastDeliverySeq')::numeric > 9007199254740991
    OR (candidate->>'schemaVersion')::integer <> expected_schema_version
    OR (candidate->>'boardId')::uuid <> expected_board_id
    OR (candidate->>'lastSeq')::bigint <> expected_snapshot_seq
    OR (candidate->>'lastDeliverySeq')::bigint <> expected_delivery_seq
  THEN
    RETURN false;
  END IF;

  FOR projected_object IN SELECT value FROM jsonb_array_elements(candidate->'objects')
  LOOP
    object_total := object_total + 1;
    IF jsonb_typeof(projected_object) <> 'object'
      OR NOT projected_object ?& ARRAY[
        'objectId', 'stackOrder', 'value', 'fieldSeq', 'createdSeq', 'updatedSeq', 'deletedSeq'
      ]
      OR projected_object - ARRAY[
        'objectId', 'stackOrder', 'value', 'fieldSeq', 'createdSeq', 'updatedSeq', 'deletedSeq'
      ]::text[] <> '{}'::jsonb
      OR jsonb_typeof(projected_object->'objectId') <> 'string'
      OR jsonb_typeof(projected_object->'stackOrder') <> 'number'
      OR jsonb_typeof(projected_object->'value') <> 'object'
      OR jsonb_typeof(projected_object->'fieldSeq') <> 'object'
      OR jsonb_typeof(projected_object->'createdSeq') <> 'number'
      OR jsonb_typeof(projected_object->'updatedSeq') <> 'number'
      OR jsonb_typeof(projected_object->'deletedSeq') NOT IN ('number', 'null')
      OR projected_object->>'stackOrder' !~ '^[1-9][0-9]*$'
      OR projected_object->>'createdSeq' !~ '^[1-9][0-9]*$'
      OR projected_object->>'updatedSeq' !~ '^[1-9][0-9]*$'
      OR (jsonb_typeof(projected_object->'deletedSeq') = 'number'
          AND projected_object->>'deletedSeq' !~ '^[1-9][0-9]*$')
      OR (projected_object->>'stackOrder')::numeric > 9007199254740991
      OR (projected_object->>'createdSeq')::numeric > 9007199254740991
      OR (projected_object->>'updatedSeq')::numeric > 9007199254740991
      OR (jsonb_typeof(projected_object->'deletedSeq') = 'number'
          AND (projected_object->>'deletedSeq')::numeric > 9007199254740991)
    THEN
      RETURN false;
    END IF;

    object_id := (projected_object->>'objectId')::uuid;
    stack_order := (projected_object->>'stackOrder')::bigint;
    created_seq := (projected_object->>'createdSeq')::bigint;
    updated_seq := (projected_object->>'updatedSeq')::bigint;
    deleted_seq := CASE WHEN jsonb_typeof(projected_object->'deletedSeq') = 'null'
      THEN NULL ELSE (projected_object->>'deletedSeq')::bigint END;
    object_value := projected_object->'value';
    field_sequences := projected_object->'fieldSeq';

    IF object_value->>'id' IS NULL
      OR (object_value->>'id')::uuid <> object_id
      OR NOT object_value ?& ARRAY['id', 'kind', 'x', 'y', 'width', 'height', 'rotation', 'fill', 'text']
      OR object_value - ARRAY['id', 'kind', 'x', 'y', 'width', 'height', 'rotation', 'fill', 'text']::text[] <> '{}'::jsonb
      OR object_value->>'kind' NOT IN ('rectangle', 'sticky')
      OR jsonb_typeof(object_value->'x') <> 'number'
      OR jsonb_typeof(object_value->'y') <> 'number'
      OR jsonb_typeof(object_value->'width') <> 'number'
      OR jsonb_typeof(object_value->'height') <> 'number'
      OR jsonb_typeof(object_value->'rotation') <> 'number'
      OR jsonb_typeof(object_value->'fill') <> 'string'
      OR jsonb_typeof(object_value->'text') <> 'string'
      OR (object_value->>'x')::numeric NOT BETWEEN -1000000 AND 1000000
      OR (object_value->>'y')::numeric NOT BETWEEN -1000000 AND 1000000
      OR (object_value->>'width')::numeric NOT BETWEEN 8 AND 100000
      OR (object_value->>'height')::numeric NOT BETWEEN 8 AND 100000
      OR (object_value->>'rotation')::numeric NOT BETWEEN -1000000 AND 1000000
      OR object_value->>'fill' !~ '^#[0-9a-fA-F]{6}$'
      OR char_length(object_value->>'text') > 10000
      OR (object_value->>'kind' = 'rectangle' AND object_value->>'text' <> '')
      OR NOT field_sequences ?& ARRAY['id', 'kind', 'x', 'y', 'width', 'height', 'rotation', 'fill', 'text']
      OR field_sequences - ARRAY['id', 'kind', 'x', 'y', 'width', 'height', 'rotation', 'fill', 'text']::text[] <> '{}'::jsonb
      OR created_seq > updated_seq
      OR updated_seq > expected_snapshot_seq
      OR (deleted_seq IS NOT NULL AND (deleted_seq <> updated_seq OR deleted_seq > expected_snapshot_seq))
      OR (previous_stack_order IS NOT NULL AND
          (stack_order < previous_stack_order OR
           (stack_order = previous_stack_order AND object_id <= previous_object_id)))
    THEN
      RETURN false;
    END IF;

    FOR field_entry IN SELECT key, value FROM jsonb_each(field_sequences)
    LOOP
      IF jsonb_typeof(field_entry.value) <> 'number'
        OR field_entry.value #>> '{}' !~ '^[1-9][0-9]*$'
        OR (field_entry.value #>> '{}')::numeric > 9007199254740991
        OR (field_entry.value #>> '{}')::bigint > expected_snapshot_seq
      THEN
        RETURN false;
      END IF;
    END LOOP;

    previous_stack_order := stack_order;
    previous_object_id := object_id;
  END LOOP;

  RETURN object_total = expected_object_count;
EXCEPTION WHEN others THEN
  RETURN false;
END;
$$;

CREATE TABLE board_snapshots (
  id uuid PRIMARY KEY,
  board_id uuid NOT NULL REFERENCES boards(id) ON DELETE CASCADE,
  snapshot_seq bigint NOT NULL CHECK (snapshot_seq >= 0 AND snapshot_seq <= 9007199254740991),
  snapshot_delivery_seq bigint NOT NULL
    CHECK (snapshot_delivery_seq >= snapshot_seq AND snapshot_delivery_seq <= 9007199254740991),
  schema_version integer NOT NULL CHECK (schema_version > 0),
  projection jsonb NOT NULL,
  canonical_hash text NOT NULL CHECK (canonical_hash ~ '^[0-9a-f]{64}$'),
  object_count integer NOT NULL CHECK (object_count >= 0),
  byte_size integer NOT NULL CHECK (byte_size BETWEEN 1 AND 16777216),
  status text NOT NULL CHECK (status IN ('creating', 'verified', 'invalid')),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  verified_at timestamptz,
  CONSTRAINT board_snapshots_board_seq_uq UNIQUE (board_id, snapshot_seq),
  CONSTRAINT board_snapshots_verification_state_check CHECK ((
    (status = 'creating' AND verified_at IS NULL)
    OR (status IN ('verified', 'invalid') AND verified_at IS NOT NULL)
  ) IS TRUE),
  CONSTRAINT board_snapshots_projection_check CHECK ((
    converge_snapshot_projection_is_valid(
      projection,
      board_id,
      snapshot_seq,
      snapshot_delivery_seq,
      schema_version,
      object_count
    )
  ) IS TRUE)
);

CREATE INDEX board_snapshots_latest_verified_idx
  ON board_snapshots(board_id, status, snapshot_seq DESC);

CREATE OR REPLACE FUNCTION converge_reject_snapshot_mutation() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF NEW.id IS DISTINCT FROM OLD.id
    OR NEW.board_id IS DISTINCT FROM OLD.board_id
    OR NEW.snapshot_seq IS DISTINCT FROM OLD.snapshot_seq
    OR NEW.snapshot_delivery_seq IS DISTINCT FROM OLD.snapshot_delivery_seq
    OR NEW.schema_version IS DISTINCT FROM OLD.schema_version
    OR NEW.projection IS DISTINCT FROM OLD.projection
    OR NEW.canonical_hash IS DISTINCT FROM OLD.canonical_hash
    OR NEW.object_count IS DISTINCT FROM OLD.object_count
    OR NEW.byte_size IS DISTINCT FROM OLD.byte_size
    OR NEW.created_at IS DISTINCT FROM OLD.created_at
  THEN
    RAISE EXCEPTION 'board snapshot content is immutable' USING ERRCODE = '55000';
  END IF;

  IF NOT (
    (OLD.status = 'creating' AND NEW.status = 'verified'
      AND OLD.verified_at IS NULL AND NEW.verified_at IS NOT NULL)
    OR (OLD.status = 'verified' AND NEW.status = 'invalid'
      AND NEW.verified_at IS NOT DISTINCT FROM OLD.verified_at)
    OR (NEW.status = OLD.status AND NEW.verified_at IS NOT DISTINCT FROM OLD.verified_at)
  ) THEN
    RAISE EXCEPTION 'invalid board snapshot status transition' USING ERRCODE = '55000';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER board_snapshots_immutable
BEFORE UPDATE ON board_snapshots
FOR EACH ROW EXECUTE FUNCTION converge_reject_snapshot_mutation();
