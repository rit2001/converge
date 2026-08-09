CREATE TABLE IF NOT EXISTS boards (
  id uuid PRIMARY KEY,
  name text NOT NULL CHECK (char_length(name) BETWEEN 1 AND 120),
  created_by uuid NOT NULL,
  last_seq bigint NOT NULL DEFAULT 0 CHECK (last_seq >= 0),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS board_members (
  board_id uuid NOT NULL REFERENCES boards(id) ON DELETE CASCADE,
  user_id uuid NOT NULL,
  role text NOT NULL CHECK (role IN ('owner', 'editor', 'viewer')),
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (board_id, user_id)
);

CREATE TABLE IF NOT EXISTS board_objects (
  board_id uuid NOT NULL REFERENCES boards(id) ON DELETE CASCADE,
  object_id uuid NOT NULL,
  kind text NOT NULL CHECK (kind IN ('rectangle', 'sticky')),
  object_data jsonb NOT NULL,
  field_seq jsonb NOT NULL,
  created_seq bigint NOT NULL CHECK (created_seq > 0),
  updated_seq bigint NOT NULL CHECK (updated_seq > 0),
  deleted_seq bigint CHECK (deleted_seq > 0),
  PRIMARY KEY (board_id, object_id)
);
CREATE INDEX IF NOT EXISTS board_objects_active_idx ON board_objects(board_id, deleted_seq);

CREATE TABLE IF NOT EXISTS board_operations (
  board_id uuid NOT NULL REFERENCES boards(id) ON DELETE CASCADE,
  seq bigint NOT NULL CHECK (seq > 0),
  op_id uuid NOT NULL,
  client_id uuid NOT NULL,
  user_id uuid NOT NULL,
  base_seq bigint NOT NULL CHECK (base_seq >= 0),
  type text NOT NULL,
  target_id uuid NOT NULL,
  command jsonb NOT NULL,
  committed_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (board_id, seq),
  CONSTRAINT board_operations_board_op_id_uq UNIQUE (board_id, op_id)
);

CREATE TABLE IF NOT EXISTS outbox_events (
  id uuid PRIMARY KEY,
  board_id uuid NOT NULL REFERENCES boards(id) ON DELETE CASCADE,
  board_seq bigint NOT NULL,
  event_type text NOT NULL,
  payload jsonb NOT NULL,
  attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
  created_at timestamptz NOT NULL DEFAULT now(),
  published_at timestamptz,
  CONSTRAINT outbox_board_seq_uq UNIQUE (board_id, board_seq)
);
CREATE INDEX IF NOT EXISTS outbox_unpublished_idx ON outbox_events(published_at, created_at);

CREATE TABLE IF NOT EXISTS converge_migrations (
  name text PRIMARY KEY,
  applied_at timestamptz NOT NULL DEFAULT now()
);
