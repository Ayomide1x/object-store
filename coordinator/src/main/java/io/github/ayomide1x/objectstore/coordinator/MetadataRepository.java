package io.github.ayomide1x.objectstore.coordinator;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/** Plain SQL over the metadata schema. Callers own transaction boundaries. */
@Repository
public class MetadataRepository {

    /** Object lifecycle states, matching the CHECK constraint on objects.status. */
    public enum Status { PENDING, COMMITTED }

    /** An objects row, without its chunks. */
    public record ObjectRow(UUID objectId, String key, long sizeBytes, Status status) {
    }

    /** A chunk as allocated at initiate, with the nodes it was placed on. */
    public record PlannedChunk(UUID chunkId, int index, long sizeBytes, List<String> placedNodeIds) {
    }

    private final JdbcClient jdbc;

    public MetadataRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public void upsertNode(Node node) {
        jdbc.sql("""
                INSERT INTO nodes (node_id, internal_addr, client_addr)
                VALUES (:nodeId, :internalAddr, :clientAddr)
                ON CONFLICT (node_id) DO UPDATE
                SET internal_addr = EXCLUDED.internal_addr, client_addr = EXCLUDED.client_addr
                """)
                .param("nodeId", node.nodeId())
                .param("internalAddr", node.internalAddr())
                .param("clientAddr", node.clientAddr())
                .update();
    }

    /** A replica row as the reconciler sees it. */
    public record ReplicaRow(String nodeId, String state) {
    }

    /** A chunk of a current COMMITTED object, with every replica row of any state. */
    public record RepairChunk(UUID chunkId, UUID objectId, int index, long sizeBytes, byte[] sha256,
            java.time.OffsetDateTime objectCreated, List<ReplicaRow> replicas) {
    }

    /**
     * Every chunk of every COMMITTED object a key currently points to, with all
     * its replica rows. Superseded versions and PENDING objects are left out:
     * they are not repaired.
     */
    public List<RepairChunk> repairCandidates() {
        record Row(UUID chunkId, UUID objectId, int index, long size, byte[] sha, java.time.OffsetDateTime created,
                String nodeId, String state) {
        }
        List<Row> rows = jdbc.sql("""
                SELECT c.chunk_id, c.object_id, c.chunk_index, c.size_bytes, c.sha256, o.created_at,
                       r.node_id, r.state
                FROM keys k
                JOIN objects o ON o.object_id = k.object_id AND o.status = 'COMMITTED'
                JOIN chunks c ON c.object_id = o.object_id
                LEFT JOIN chunk_replicas r ON r.chunk_id = c.chunk_id
                ORDER BY c.chunk_id, r.node_id
                """)
                .query((rs, i) -> new Row(rs.getObject("chunk_id", UUID.class), rs.getObject("object_id", UUID.class),
                        rs.getInt("chunk_index"), rs.getLong("size_bytes"), rs.getBytes("sha256"),
                        rs.getObject("created_at", java.time.OffsetDateTime.class),
                        rs.getString("node_id"), rs.getString("state")))
                .list();
        Map<UUID, RepairChunk> out = new LinkedHashMap<>();
        for (Row r : rows) {
            RepairChunk c = out.computeIfAbsent(r.chunkId(), id -> new RepairChunk(id, r.objectId(), r.index(),
                    r.size(), r.sha(), r.created(), new ArrayList<>()));
            if (r.nodeId() != null) {
                c.replicas().add(new ReplicaRow(r.nodeId(), r.state()));
            }
        }
        return List.copyOf(out.values());
    }

    /** Records a replica repair confirmed on the target. Returns rows inserted (0 if one already existed). */
    public int insertRepairedReplica(UUID chunkId, String nodeId, String origin) {
        return jdbc.sql("""
                INSERT INTO chunk_replicas (chunk_id, node_id, origin) VALUES (:chunkId, :nodeId, :origin)
                ON CONFLICT (chunk_id, node_id) DO NOTHING
                """)
                .param("chunkId", chunkId).param("nodeId", nodeId).param("origin", origin).update();
    }

    /**
     * Records that a target's own StatChunks reported an invalid file under the
     * chunk's name: a BAD row, so no pass targets that node for the chunk, and
     * stage 8 can find the file.
     */
    public int insertBadReplicaFound(UUID chunkId, String nodeId, String reason) {
        return jdbc.sql("""
                INSERT INTO chunk_replicas (chunk_id, node_id, state, bad_reason, marked_bad_at, origin)
                VALUES (:chunkId, :nodeId, 'BAD', :reason, clock_timestamp(), 'repair')
                ON CONFLICT (chunk_id, node_id) DO NOTHING
                """)
                .param("chunkId", chunkId).param("nodeId", nodeId).param("reason", reason).update();
    }

    public long startRepairPass() {
        return jdbc.sql("INSERT INTO repair_passes DEFAULT VALUES RETURNING pass_id").query(Long.class).single();
    }

    public void finishRepairPass(long passId, String skippedReason, int underReplicated, int copiesStarted,
            int recorded, int foundRecorded, int badFound, int failures) {
        jdbc.sql("""
                UPDATE repair_passes SET finished_at = clock_timestamp(), skipped_reason = :skipped,
                  under_replicated = :under, copies_started = :copies, replicas_recorded = :recorded,
                  found_recorded = :found, bad_found = :bad, failures = :failures
                WHERE pass_id = :id
                """)
                .param("skipped", skippedReason).param("under", underReplicated).param("copies", copiesStarted)
                .param("recorded", recorded).param("found", foundRecorded).param("bad", badFound)
                .param("failures", failures).param("id", passId).update();
    }

    /** Deletes repair_passes rows started more than retention ago. Returns rows deleted. */
    public int pruneRepairPasses(java.time.Duration retention) {
        return jdbc.sql("DELETE FROM repair_passes WHERE started_at < clock_timestamp() - make_interval(secs => :secs)")
                .param("secs", (double) retention.toMillis() / 1000).update();
    }

    /** Persisted liveness, for observability: written as UNKNOWN at coordinator startup. */
    public void markAllNodesUnknown() {
        jdbc.sql("UPDATE nodes SET state = 'UNKNOWN', state_changed_at = clock_timestamp()").update();
    }

    public void updateNodeState(String nodeId, String state) {
        jdbc.sql("UPDATE nodes SET state = :state, state_changed_at = clock_timestamp() WHERE node_id = :id")
                .param("state", state).param("id", nodeId).update();
    }

    public void recordHeartbeat(String nodeId, String instanceId) {
        jdbc.sql("UPDATE nodes SET last_heartbeat_at = clock_timestamp(), instance_id = :inst WHERE node_id = :id")
                .param("inst", instanceId).param("id", nodeId).update();
    }

    public void recordNodeInstance(String nodeId, String instanceId) {
        jdbc.sql("UPDATE nodes SET instance_id = :inst WHERE node_id = :id")
                .param("inst", instanceId).param("id", nodeId).update();
    }

    public List<Node> listNodes() {
        return jdbc.sql("SELECT node_id, internal_addr, client_addr FROM nodes ORDER BY node_id")
                .query((rs, i) -> new Node(rs.getString("node_id"), rs.getString("internal_addr"),
                        rs.getString("client_addr")))
                .list();
    }

    public void insertPendingObject(UUID objectId, String key, long sizeBytes) {
        jdbc.sql("""
                INSERT INTO objects (object_id, key, size_bytes, status)
                VALUES (:objectId, :key, :sizeBytes, 'PENDING')
                """)
                .param("objectId", objectId)
                .param("key", key)
                .param("sizeBytes", sizeBytes)
                .update();
    }

    public void insertChunk(UUID chunkId, UUID objectId, int index, long sizeBytes) {
        jdbc.sql("""
                INSERT INTO chunks (chunk_id, object_id, chunk_index, size_bytes)
                VALUES (:chunkId, :objectId, :index, :sizeBytes)
                """)
                .param("chunkId", chunkId)
                .param("objectId", objectId)
                .param("index", index)
                .param("sizeBytes", sizeBytes)
                .update();
    }

    public void insertPlacement(UUID chunkId, String nodeId) {
        jdbc.sql("INSERT INTO chunk_placements (chunk_id, node_id) VALUES (:chunkId, :nodeId)")
                .param("chunkId", chunkId)
                .param("nodeId", nodeId)
                .update();
    }

    /** Reads an objects row without locking it. */
    public Optional<ObjectRow> findObject(UUID objectId) {
        return jdbc.sql("SELECT object_id, key, size_bytes, status FROM objects WHERE object_id = :id")
                .param("id", objectId)
                .query((rs, i) -> new ObjectRow(
                        rs.getObject("object_id", UUID.class),
                        rs.getString("key"),
                        rs.getLong("size_bytes"),
                        Status.valueOf(rs.getString("status"))))
                .optional();
    }

    /**
     * Locks an object row for the rest of the transaction, so two completes of
     * the same upload run one after the other.
     */
    public Optional<ObjectRow> lockObject(UUID objectId) {
        return jdbc.sql("SELECT object_id, key, size_bytes, status FROM objects WHERE object_id = :id FOR UPDATE")
                .param("id", objectId)
                .query((rs, i) -> new ObjectRow(
                        rs.getObject("object_id", UUID.class),
                        rs.getString("key"),
                        rs.getLong("size_bytes"),
                        Status.valueOf(rs.getString("status"))))
                .optional();
    }

    /** The chunk plan issued at initiate, in index order. */
    public List<PlannedChunk> plannedChunks(UUID objectId) {
        record Row(UUID chunkId, int index, long sizeBytes, String nodeId) {
        }
        List<Row> rows = jdbc.sql("""
                SELECT c.chunk_id, c.chunk_index, c.size_bytes, p.node_id
                FROM chunks c
                LEFT JOIN chunk_placements p ON p.chunk_id = c.chunk_id
                WHERE c.object_id = :id
                ORDER BY c.chunk_index, p.node_id
                """)
                .param("id", objectId)
                .query((rs, i) -> new Row(
                        rs.getObject("chunk_id", UUID.class),
                        rs.getInt("chunk_index"),
                        rs.getLong("size_bytes"),
                        rs.getString("node_id")))
                .list();

        Map<UUID, PlannedChunk> chunks = new LinkedHashMap<>();
        for (Row row : rows) {
            PlannedChunk chunk = chunks.computeIfAbsent(row.chunkId(),
                    id -> new PlannedChunk(id, row.index(), row.sizeBytes(), new ArrayList<>()));
            if (row.nodeId() != null) {
                chunk.placedNodeIds().add(row.nodeId());
            }
        }
        return List.copyOf(chunks.values());
    }

    public void insertReplica(UUID chunkId, String nodeId) {
        jdbc.sql("INSERT INTO chunk_replicas (chunk_id, node_id) VALUES (:chunkId, :nodeId)")
                .param("chunkId", chunkId)
                .param("nodeId", nodeId)
                .update();
    }

    /** A replica's state ("HEALTHY" or "BAD"), or empty if no such replica is recorded. */
    public Optional<String> replicaState(UUID chunkId, String nodeId) {
        return jdbc.sql("SELECT state FROM chunk_replicas WHERE chunk_id = :chunkId AND node_id = :nodeId")
                .param("chunkId", chunkId)
                .param("nodeId", nodeId)
                .query(String.class)
                .optional();
    }

    /**
     * Marks a HEALTHY replica BAD. Returns the rows changed: 0 if it was
     * already BAD (or gone), so of two concurrent marks exactly one wins.
     */
    public int markReplicaBad(UUID chunkId, String nodeId, String reason) {
        return jdbc.sql("""
                UPDATE chunk_replicas
                SET state = 'BAD', bad_reason = :reason, marked_bad_at = clock_timestamp()
                WHERE chunk_id = :chunkId AND node_id = :nodeId AND state = 'HEALTHY'
                """)
                .param("chunkId", chunkId)
                .param("nodeId", nodeId)
                .param("reason", reason)
                .update();
    }

    /** The recorded SHA-256 of a chunk, or empty if it has none (still PENDING) or doesn't exist. */
    public Optional<byte[]> chunkHash(UUID chunkId) {
        return jdbc.sql("SELECT sha256 FROM chunks WHERE chunk_id = :id AND sha256 IS NOT NULL")
                .param("id", chunkId)
                .query((rs, i) -> rs.getBytes(1))
                .optional();
    }

    /** "node_id: reason" for each BAD replica of each chunk, for error messages. */
    public Map<UUID, List<String>> badReplicaReasons(List<UUID> chunkIds) {
        Map<UUID, List<String>> result = new LinkedHashMap<>();
        if (chunkIds.isEmpty()) {
            return result;
        }
        jdbc.sql("""
                SELECT chunk_id, node_id, bad_reason FROM chunk_replicas
                WHERE chunk_id IN (:ids) AND state = 'BAD' ORDER BY chunk_id, node_id
                """)
                .param("ids", chunkIds)
                .query((rs, i) -> {
                    result.computeIfAbsent(rs.getObject("chunk_id", UUID.class), id -> new ArrayList<>())
                            .add(rs.getString("node_id") + ": " + rs.getString("bad_reason"));
                    return null;
                })
                .list();
        return result;
    }

    /** Records a chunk's SHA-256. Set once: a chunk's hash never changes. */
    public void recordChunkHash(UUID chunkId, byte[] sha256) {
        jdbc.sql("UPDATE chunks SET sha256 = :sha256 WHERE chunk_id = :chunkId AND sha256 IS NULL")
                .param("chunkId", chunkId)
                .param("sha256", sha256)
                .update();
    }

    public void markCommitted(UUID objectId) {
        jdbc.sql("""
                UPDATE objects SET status = 'COMMITTED', committed_at = clock_timestamp()
                WHERE object_id = :id AND status = 'PENDING'
                """)
                .param("id", objectId)
                .update();
    }

    /**
     * Points key at objectId, inserting or replacing the pointer atomically.
     * Concurrent swaps of one key serialize on the keys row (or, for a new
     * key, on its primary-key index entry). The transaction that commits last
     * wins.
     */
    public void swapKey(String key, UUID objectId) {
        jdbc.sql("""
                INSERT INTO keys (key, object_id) VALUES (:key, :objectId)
                ON CONFLICT (key) DO UPDATE
                SET object_id = EXCLUDED.object_id, updated_at = clock_timestamp()
                """)
                .param("key", key)
                .param("objectId", objectId)
                .update();
    }

    /**
     * Resolves a key and loads that object's chunks and HEALTHY replicas in
     * one statement, so the result is a single snapshot of exactly one object.
     * Chunks and replicas are LEFT JOINed: a 0-byte object has no chunks but
     * still resolves, and a chunk whose replicas are all BAD comes back with
     * none.
     */
    public Optional<StoredObject> findObjectByKey(String key) {
        record Row(UUID objectId, long objectSize, UUID chunkId, int chunkIndex, long chunkSize, byte[] sha256,
                Node node) {
        }
        List<Row> rows = jdbc.sql("""
                SELECT o.object_id, o.size_bytes AS object_size,
                       c.chunk_id, c.chunk_index, c.size_bytes AS chunk_size, c.sha256,
                       n.node_id, n.internal_addr, n.client_addr
                FROM keys k
                JOIN objects o ON o.object_id = k.object_id AND o.status = 'COMMITTED'
                LEFT JOIN chunks c ON c.object_id = o.object_id
                LEFT JOIN chunk_replicas r ON r.chunk_id = c.chunk_id AND r.state = 'HEALTHY'
                LEFT JOIN nodes n ON n.node_id = r.node_id
                WHERE k.key = :key
                ORDER BY c.chunk_index, n.node_id
                """)
                .param("key", key)
                .query((rs, i) -> new Row(
                        rs.getObject("object_id", UUID.class),
                        rs.getLong("object_size"),
                        rs.getObject("chunk_id", UUID.class),
                        rs.getInt("chunk_index"),
                        rs.getLong("chunk_size"),
                        rs.getBytes("sha256"),
                        rs.getString("node_id") == null ? null
                                : new Node(rs.getString("node_id"), rs.getString("internal_addr"),
                                        rs.getString("client_addr"))))
                .list();
        if (rows.isEmpty()) {
            return Optional.empty();
        }

        Map<UUID, StoredObject.Chunk> chunks = new LinkedHashMap<>();
        for (Row row : rows) {
            if (row.chunkId() == null) {
                continue; // the single row of an object with no chunks
            }
            StoredObject.Chunk chunk = chunks.computeIfAbsent(row.chunkId(),
                    id -> new StoredObject.Chunk(id, row.chunkIndex(), row.chunkSize(), row.sha256(),
                            new ArrayList<>()));
            if (row.node() != null) {
                chunk.replicas().add(row.node());
            }
        }
        Row first = rows.getFirst();
        return Optional.of(new StoredObject(first.objectId(), first.objectSize(), List.copyOf(chunks.values())));
    }
}
