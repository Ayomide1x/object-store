-- Stage 11: a history of liveness transitions, for the web UI's activity feed.
--
-- nodes holds only each node's current state. This table records every
-- transition the tracker makes. It is written asynchronously, after the
-- transition, from a bounded queue: a slow or failing write never blocks or
-- delays liveness, so an event can be missing (the coordinator counts and logs
-- each one it drops). Never read to decide anything.
--
-- from_state is NULL for the events written at coordinator startup, when every
-- node becomes UNKNOWN. Rows older than COORDINATOR_LIVENESS_EVENT_RETENTION
-- are deleted at the start of each repair pass.
CREATE TABLE liveness_events (
    event_id   bigserial PRIMARY KEY,
    node_id    text NOT NULL REFERENCES nodes (node_id),
    from_state text CHECK (from_state IN ('UNKNOWN', 'ALIVE', 'SUSPECT', 'DEAD')),
    to_state   text NOT NULL CHECK (to_state IN ('UNKNOWN', 'ALIVE', 'SUSPECT', 'DEAD')),
    at         timestamptz NOT NULL
);
CREATE INDEX liveness_events_at ON liveness_events (at);
