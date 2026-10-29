-- Stage 2: object lifecycle (PENDING -> COMMITTED) and a key -> object pointer
-- that is swapped atomically on overwrite.

-- Keys are no longer unique on objects: an overwrite creates a second object
-- for the same key. objects.key is the key an upload targets; the keys table
-- says which object a key resolves to.
ALTER TABLE objects DROP CONSTRAINT objects_key_unique;

-- The default exists only to backfill stage 1 rows, which were all complete.
ALTER TABLE objects ADD COLUMN status text NOT NULL DEFAULT 'COMMITTED'
    CHECK (status IN ('PENDING', 'COMMITTED'));
ALTER TABLE objects ALTER COLUMN status DROP DEFAULT;

-- Set with clock_timestamp() at commit. Informational only: it does not define
-- version order between objects for the same key. The keys row does.
ALTER TABLE objects ADD COLUMN committed_at timestamptz;
UPDATE objects SET committed_at = created_at;

-- Target of the composite foreign key from keys.
ALTER TABLE objects ADD CONSTRAINT objects_id_status_unique UNIQUE (object_id, status);

-- Nodes each chunk was placed on at initiate. CompleteUpload accepts an
-- acknowledgement only from a placed node.
CREATE TABLE chunk_placements (
    chunk_id uuid NOT NULL REFERENCES chunks (chunk_id),
    node_id  text NOT NULL REFERENCES nodes (node_id),
    PRIMARY KEY (chunk_id, node_id)
);
INSERT INTO chunk_placements (chunk_id, node_id)
SELECT chunk_id, node_id FROM chunk_replicas;

-- The key pointer. The composite foreign key means a key can only reference a
-- COMMITTED object (invariant 1), enforced by Postgres. updated_at is set with
-- clock_timestamp() and, like committed_at, does not define version order.
CREATE TABLE keys (
    key           text PRIMARY KEY,
    object_id     uuid NOT NULL,
    object_status text NOT NULL DEFAULT 'COMMITTED' CHECK (object_status = 'COMMITTED'),
    updated_at    timestamptz NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (object_id, object_status) REFERENCES objects (object_id, status)
);
INSERT INTO keys (key, object_id)
SELECT key, object_id FROM objects;
