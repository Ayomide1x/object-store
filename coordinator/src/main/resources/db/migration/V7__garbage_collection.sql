-- Stage 8: deletes and garbage collection.
--
-- An object leaves the readable world by one row-locked transition:
--   COMMITTED -> SUPERSEDED  found by GC: COMMITTED and no key points to it
--   COMMITTED -> DELETED     osctl rm, in the same transaction that removes the key
--   PENDING   -> ABANDONED   found by GC: PENDING longer than the pending timeout
-- None of the three is ever pointed to by a key again. The composite foreign
-- key from keys to objects (object_id, status) is unchanged: it already makes
-- "remove the key first" the only way out of COMMITTED.
ALTER TABLE objects DROP CONSTRAINT objects_status_check;
ALTER TABLE objects ADD CONSTRAINT objects_status_check
    CHECK (status IN ('PENDING', 'COMMITTED', 'SUPERSEDED', 'DELETED', 'ABANDONED'));

-- retired_at: when the object left COMMITTED or PENDING (the read grace runs
-- from here). gc_started_at: when its deletion tasks were first created.
ALTER TABLE objects
    ADD COLUMN retired_at timestamptz,
    ADD COLUMN gc_started_at timestamptz,
    ADD CONSTRAINT objects_retired_has_time
        CHECK ((status IN ('SUPERSEDED', 'DELETED', 'ABANDONED')) = (retired_at IS NOT NULL));
CREATE INDEX objects_status_created ON objects (status, created_at);

-- Every place a chunk's file may be: 'initiate' (the upload plan) or 'repair'
-- (a copy target, recorded before the copy is ordered). GC deletes from every
-- placement, so a copy that landed without a replica row is still found.
ALTER TABLE chunk_placements
    ADD COLUMN origin text NOT NULL DEFAULT 'initiate' CHECK (origin IN ('initiate', 'repair'));

-- One durable task per chunk file to delete on one node. A task is removed,
-- together with that node's replica row and placement, only when the node
-- itself confirms the file is gone. Survives a coordinator restart.
CREATE TABLE pending_deletions (
    chunk_id   uuid NOT NULL REFERENCES chunks (chunk_id),
    node_id    text NOT NULL REFERENCES nodes (node_id),
    -- true when the chunk's object is gone (the node then refuses the ID
    -- forever); false for a BAD copy of a live chunk.
    tombstone  boolean NOT NULL,
    reason     text NOT NULL CHECK (reason IN ('superseded', 'deleted', 'abandoned', 'bad-replica')),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    attempts   int NOT NULL DEFAULT 0,
    last_error text,
    PRIMARY KEY (chunk_id, node_id)
);
CREATE INDEX pending_deletions_node ON pending_deletions (node_id);

-- One row per GC pass, like repair_passes, pruned by retention.
CREATE TABLE gc_passes (
    pass_id          bigserial PRIMARY KEY,
    started_at       timestamptz NOT NULL DEFAULT clock_timestamp(),
    finished_at      timestamptz,
    skipped_reason   text,
    abandoned        int NOT NULL DEFAULT 0,
    superseded       int NOT NULL DEFAULT 0,
    tasks_created    int NOT NULL DEFAULT 0,
    files_deleted    int NOT NULL DEFAULT 0,
    already_absent   int NOT NULL DEFAULT 0,
    failures         int NOT NULL DEFAULT 0,
    objects_removed  int NOT NULL DEFAULT 0
);
CREATE INDEX gc_passes_started_at ON gc_passes (started_at);
