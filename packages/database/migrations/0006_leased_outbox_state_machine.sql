ALTER TABLE outbox_events
  RENAME COLUMN attempts TO attempt_count;

ALTER TABLE outbox_events
  DROP CONSTRAINT outbox_events_attempts_check;

ALTER TABLE outbox_events
  ADD COLUMN status text,
  ADD COLUMN lease_owner text,
  ADD COLUMN lease_token uuid,
  ADD COLUMN leased_until timestamptz,
  ADD COLUMN next_attempt_at timestamptz,
  ADD COLUMN redis_entry_id text,
  ADD COLUMN last_error_code text,
  ADD COLUMN last_error_message text,
  ADD COLUMN last_error_at timestamptz,
  ADD COLUMN updated_at timestamptz;

UPDATE outbox_events
SET status = CASE WHEN published_at IS NULL THEN 'pending' ELSE 'published' END,
    next_attempt_at = CASE
      WHEN published_at IS NULL THEN created_at
      ELSE 'infinity'::timestamptz
    END,
    redis_entry_id = CASE WHEN published_at IS NULL THEN NULL ELSE 'legacy_backfill' END,
    updated_at = COALESCE(published_at, created_at);

ALTER TABLE outbox_events
  ALTER COLUMN status SET DEFAULT 'pending',
  ALTER COLUMN status SET NOT NULL,
  ALTER COLUMN next_attempt_at SET DEFAULT now(),
  ALTER COLUMN next_attempt_at SET NOT NULL,
  ALTER COLUMN updated_at SET DEFAULT now(),
  ALTER COLUMN updated_at SET NOT NULL,
  ADD CONSTRAINT outbox_status_check CHECK (
    status IN ('pending', 'leased', 'retry_wait', 'published', 'blocked')
  ),
  ADD CONSTRAINT outbox_attempt_count_check CHECK (
    attempt_count BETWEEN 0 AND 20
    AND (status NOT IN ('leased', 'retry_wait', 'blocked') OR attempt_count > 0)
    AND (status NOT IN ('pending', 'retry_wait') OR attempt_count < 20)
  ),
  ADD CONSTRAINT outbox_lease_state_check CHECK ((
    (
      status = 'leased'
      AND lease_owner IS NOT NULL
      AND lease_token IS NOT NULL
      AND leased_until IS NOT NULL
      AND leased_until > '-infinity'::timestamptz
      AND leased_until < 'infinity'::timestamptz
    )
    OR (
      status <> 'leased'
      AND lease_owner IS NULL
      AND lease_token IS NULL
      AND leased_until IS NULL
    )
  ) IS TRUE),
  ADD CONSTRAINT outbox_lease_owner_length_check CHECK (
    lease_owner IS NULL OR char_length(lease_owner) BETWEEN 1 AND 128
  ),
  ADD CONSTRAINT outbox_publication_state_check CHECK ((
    (
      status = 'published'
      AND published_at IS NOT NULL
      AND redis_entry_id IS NOT NULL
      AND char_length(redis_entry_id) BETWEEN 1 AND 128
    )
    OR (
      status <> 'published'
      AND published_at IS NULL
      AND redis_entry_id IS NULL
    )
  ) IS TRUE),
  ADD CONSTRAINT outbox_next_attempt_state_check CHECK ((
    (
      status IN ('published', 'blocked')
      AND next_attempt_at = 'infinity'::timestamptz
    )
    OR (
      status IN ('pending', 'leased', 'retry_wait')
      AND next_attempt_at > '-infinity'::timestamptz
      AND next_attempt_at < 'infinity'::timestamptz
    )
  ) IS TRUE),
  ADD CONSTRAINT outbox_error_state_check CHECK ((
    (
      last_error_code IS NULL
      AND last_error_message IS NULL
      AND last_error_at IS NULL
    )
    OR (
      last_error_code IS NOT NULL
      AND char_length(last_error_code) BETWEEN 1 AND 64
      AND last_error_message IS NOT NULL
      AND char_length(last_error_message) BETWEEN 1 AND 500
      AND last_error_at IS NOT NULL
    )
  ) IS TRUE),
  ADD CONSTRAINT outbox_failure_state_check CHECK ((
    (status NOT IN ('retry_wait', 'blocked'))
    OR (
      last_error_code IS NOT NULL
      AND last_error_message IS NOT NULL
      AND last_error_at IS NOT NULL
    )
  ) IS TRUE),
  ADD CONSTRAINT outbox_published_error_check CHECK (
    status <> 'published'
    OR (
      last_error_code IS NULL
      AND last_error_message IS NULL
      AND last_error_at IS NULL
    )
  );

CREATE UNIQUE INDEX outbox_lease_token_uq
  ON outbox_events(lease_token)
  WHERE lease_token IS NOT NULL;

CREATE INDEX outbox_eligible_idx
  ON outbox_events(next_attempt_at, created_at, id)
  WHERE status IN ('pending', 'retry_wait');

CREATE INDEX outbox_lease_recovery_idx
  ON outbox_events(leased_until, created_at, id)
  WHERE status = 'leased';

CREATE INDEX outbox_board_predecessor_idx
  ON outbox_events(board_id, delivery_seq, status);

CREATE INDEX outbox_status_created_idx
  ON outbox_events(status, created_at);

CREATE INDEX outbox_published_at_idx
  ON outbox_events(published_at)
  WHERE status = 'published';

DROP INDEX outbox_unpublished_idx;
