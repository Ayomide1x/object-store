-- Stage 3: per-chunk SHA-256.
--
-- Chunks written before stage 3 have no checksum, and their files on the node
-- have no header to verify against. Serving them would break invariant 5, and
-- hashing them now would only bless whatever is on disk. So stage 3 starts
-- from a clean volume, and this migration refuses to run otherwise.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM chunks) THEN
        RAISE EXCEPTION 'stage 3 requires a clean volume: % existing chunks have no checksum. '
                        'Run: docker compose down -v', (SELECT count(*) FROM chunks);
    END IF;
END $$;

-- NULL while the object is PENDING (the hash is not known at initiate); set at
-- CompleteUpload from the hash the client reports for each chunk.
ALTER TABLE chunks ADD COLUMN sha256 bytea CHECK (octet_length(sha256) = 32);
