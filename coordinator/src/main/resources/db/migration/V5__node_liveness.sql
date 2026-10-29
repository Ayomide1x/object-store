-- Stage 6: node liveness, from heartbeats.
--
-- The coordinator's in-memory tracker is authoritative and judges liveness by
-- its own monotonic clock. These columns are a persisted copy, written on each
-- transition (last_heartbeat_at at most every 10 s per node), for observability,
-- for scripts, and for stage 7, which needs to know how long a node has been
-- DEAD. All times are wall-clock (clock_timestamp()) and never drive
-- transitions. At coordinator startup every node is written as UNKNOWN.
ALTER TABLE nodes
    ADD COLUMN state text NOT NULL DEFAULT 'UNKNOWN' CHECK (state IN ('UNKNOWN', 'ALIVE', 'SUSPECT', 'DEAD')),
    ADD COLUMN state_changed_at timestamptz,
    ADD COLUMN last_heartbeat_at timestamptz,
    ADD COLUMN instance_id text;
