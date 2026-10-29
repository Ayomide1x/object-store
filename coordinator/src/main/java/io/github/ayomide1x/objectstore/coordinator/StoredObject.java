package io.github.ayomide1x.objectstore.coordinator;

import java.util.List;
import java.util.UUID;

/** An object as recorded in Postgres, with its chunks in index order. */
public record StoredObject(UUID objectId, long sizeBytes, List<Chunk> chunks) {

    /** A chunk, its SHA-256, and the nodes holding an acknowledged replica of it. */
    public record Chunk(UUID chunkId, int index, long sizeBytes, byte[] sha256, List<Node> replicas) {
    }
}
