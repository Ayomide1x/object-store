-- Stage 7: re-replication.
--
-- origin says how a replica row came to exist: 'upload' (verified at
-- CompleteUpload), 'repair' (a copy the reconciler ordered, confirmed by the
-- target's own StatChunks), or 'repair-found' (a valid file the reconciler
-- found on a target that had no row, for example a copy that finished as the
-- coordinator restarted). Repair also inserts BAD rows for invalid files it
-- finds on a target, so no later pass targets that node for the chunk.
ALTER TABLE chunk_replicas
    ADD COLUMN origin text NOT NULL DEFAULT 'upload' CHECK (origin IN ('upload', 'repair', 'repair-found'));

-- One row per reconciler pass, for observability and the acceptance scripts.
-- Rows older than COORDINATOR_REPAIR_PASS_RETENTION are deleted at the start
-- of each pass.
CREATE TABLE repair_passes (
    pass_id           bigserial PRIMARY KEY,
    started_at        timestamptz NOT NULL DEFAULT clock_timestamp(),
    finished_at       timestamptz,
    skipped_reason    text,
    under_replicated  int NOT NULL DEFAULT 0,
    copies_started    int NOT NULL DEFAULT 0,
    replicas_recorded int NOT NULL DEFAULT 0,
    found_recorded    int NOT NULL DEFAULT 0,
    bad_found         int NOT NULL DEFAULT 0,
    failures          int NOT NULL DEFAULT 0
);
CREATE INDEX repair_passes_started_at ON repair_passes (started_at);
