-- Storage nodes. internal_addr is used inside the cluster network; client_addr
-- is what clients outside it (osctl on the host) are told to dial.
CREATE TABLE nodes (
    node_id       text PRIMARY KEY,
    internal_addr text NOT NULL,
    client_addr   text NOT NULL
);

-- One row per stored object. The unique constraint on key is what rejects a
-- second put of the same key, including two concurrent ones.
CREATE TABLE objects (
    object_id  uuid PRIMARY KEY,
    key        text NOT NULL,
    size_bytes bigint NOT NULL CHECK (size_bytes >= 0),
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT objects_key_unique UNIQUE (key)
);

CREATE TABLE chunks (
    chunk_id    uuid PRIMARY KEY,
    object_id   uuid NOT NULL REFERENCES objects (object_id),
    chunk_index int NOT NULL CHECK (chunk_index >= 0),
    size_bytes  bigint NOT NULL CHECK (size_bytes >= 0),
    CONSTRAINT chunks_object_index_unique UNIQUE (object_id, chunk_index)
);

-- A row exists only for a replica whose node acknowledged the write.
CREATE TABLE chunk_replicas (
    chunk_id   uuid NOT NULL REFERENCES chunks (chunk_id),
    node_id    text NOT NULL REFERENCES nodes (node_id),
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (chunk_id, node_id)
);
