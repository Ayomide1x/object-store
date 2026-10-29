-- Stage 5: a replica can be marked BAD once its own node confirms, on a
-- client's report, that the copy is corrupt or missing.
--
-- BAD is terminal: there is no way back to HEALTHY. Readers are never directed
-- to a BAD replica (GetObject leaves them out); stage 7 counts only HEALTHY
-- replicas and copies only from them; stage 8 finds BAD rows to delete the
-- file on the node, then the row. Existing replicas are all HEALTHY.
ALTER TABLE chunk_replicas
    ADD COLUMN state text NOT NULL DEFAULT 'HEALTHY' CHECK (state IN ('HEALTHY', 'BAD')),
    ADD COLUMN bad_reason text,
    ADD COLUMN marked_bad_at timestamptz,
    ADD CONSTRAINT chunk_replicas_bad_has_reason
        CHECK ((state = 'BAD') = (bad_reason IS NOT NULL AND marked_bad_at IS NOT NULL));
