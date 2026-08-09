ALTER TABLE board_objects
  ADD COLUMN IF NOT EXISTS stack_order bigint;

UPDATE board_objects
SET stack_order = created_seq
WHERE stack_order IS NULL;

ALTER TABLE board_objects
  ALTER COLUMN stack_order SET NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS board_objects_stack_order_uq
  ON board_objects(board_id, stack_order);
