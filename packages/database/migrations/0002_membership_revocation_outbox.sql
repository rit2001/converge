ALTER TABLE outbox_events
  DROP CONSTRAINT IF EXISTS outbox_board_seq_uq;

ALTER TABLE outbox_events
  ALTER COLUMN board_seq DROP NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS outbox_operation_board_seq_uq
  ON outbox_events(board_id, board_seq)
  WHERE event_type = 'operation.committed';
