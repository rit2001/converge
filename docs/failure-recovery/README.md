# Failure recovery

Milestone 1 uses a room-first join, fixed sequence watermark, bounded operation-log catch-up, and live
event buffering before reporting `READY`. The same protocol runs for initial load and reconnect, and
duplicate catch-up/live delivery is harmless. Crash-window outbox replay, Redis fan-out, server-crash
recovery, and snapshot-tail recovery remain deferred to Milestone 2.
