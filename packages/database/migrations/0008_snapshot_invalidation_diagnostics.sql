ALTER TABLE board_snapshots
  ADD COLUMN invalidation_code text,
  ADD COLUMN invalidated_at timestamptz;

UPDATE board_snapshots
SET invalidation_code = 'LEGACY_INVALID',
    invalidated_at = clock_timestamp()
WHERE status = 'invalid';

ALTER TABLE board_snapshots
  ADD CONSTRAINT board_snapshots_invalidation_state_check CHECK ((
    (status IN ('creating', 'verified')
      AND invalidation_code IS NULL
      AND invalidated_at IS NULL)
    OR (status = 'invalid'
      AND invalidation_code ~ '^[A-Z][A-Z0-9_]{0,63}$'
      AND invalidated_at IS NOT NULL)
  ) IS TRUE);

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
      AND OLD.verified_at IS NULL AND NEW.verified_at IS NOT NULL
      AND NEW.invalidation_code IS NULL AND NEW.invalidated_at IS NULL)
    OR (OLD.status = 'verified' AND NEW.status = 'invalid'
      AND NEW.verified_at IS NOT DISTINCT FROM OLD.verified_at
      AND NEW.invalidation_code ~ '^[A-Z][A-Z0-9_]{0,63}$'
      AND NEW.invalidated_at IS NOT NULL)
    OR (NEW.status = OLD.status
      AND NEW.verified_at IS NOT DISTINCT FROM OLD.verified_at
      AND NEW.invalidation_code IS NOT DISTINCT FROM OLD.invalidation_code
      AND NEW.invalidated_at IS NOT DISTINCT FROM OLD.invalidated_at)
  ) THEN
    RAISE EXCEPTION 'invalid board snapshot status transition' USING ERRCODE = '55000';
  END IF;
  RETURN NEW;
END;
$$;
