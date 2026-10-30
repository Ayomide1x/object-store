package io.github.ayomide1x.objectstore.coordinator;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
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

    /**
     * Object lifecycle states, matching the CHECK constraint on objects.status.
     * SUPERSEDED, DELETED and ABANDONED are retired: no key points to them, ever
     * again, and garbage collection deletes their files.
     */
    public enum Status {
        PENDING, COMMITTED, SUPERSEDED, DELETED, ABANDONED;

        boolean retired() {
            return this == SUPERSEDED || this == DELETED || this == ABANDONED;
        }
    }

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

    /**
     * Locks an object row in share mode for the rest of the transaction and
     * reports whether it is current: COMMITTED and pointed to by a key. Repair
     * calls this before recording a placement or a replica. Retiring an object
     * (rm, or GC finding it superseded) updates the row, so it waits for this
     * lock, and a lock taken after it sees the new status. A placement or row
     * recorded under the lock is therefore always visible to GC's task creation.
     */
    public boolean lockIfCurrent(UUID objectId) {
        return jdbc.sql("""
                SELECT 1 FROM objects o
                WHERE o.object_id = :id AND o.status = 'COMMITTED'
                  AND EXISTS (SELECT 1 FROM keys k WHERE k.object_id = o.object_id)
                FOR SHARE OF o
                """)
                .param("id", objectId).query(Integer.class).optional().isPresent();
    }

    /** Records that repair is about to copy a chunk to nodeId. Returns rows inserted. */
    public int insertRepairPlacement(UUID chunkId, String nodeId) {
        return jdbc.sql("""
                INSERT INTO chunk_placements (chunk_id, node_id, origin) VALUES (:chunkId, :nodeId, 'repair')
                ON CONFLICT (chunk_id, node_id) DO NOTHING
                """)
                .param("chunkId", chunkId).param("nodeId", nodeId).update();
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

    /** Inserts liveness events, one row each. Called only by {@link LivenessEventLog}'s writer. */
    public void insertLivenessEvents(List<LivenessEventLog.Event> events) {
        for (LivenessEventLog.Event e : events) {
            jdbc.sql("INSERT INTO liveness_events (node_id, from_state, to_state, at) VALUES (:id, :from, :to, :at)")
                    .param("id", e.nodeId()).param("from", e.from() == null ? null : e.from().name())
                    .param("to", e.to().name()).param("at", java.time.OffsetDateTime.ofInstant(e.at(), java.time.ZoneOffset.UTC))
                    .update();
        }
    }

    public int pruneLivenessEvents(java.time.Duration retention) {
        return jdbc.sql("DELETE FROM liveness_events WHERE at < clock_timestamp() - make_interval(secs => :secs)")
                .param("secs", (double) retention.toMillis() / 1000).update();
    }

    /** A liveness_events row. from is null for the events written at coordinator startup. */
    public record LivenessEventRow(long eventId, String nodeId, String from, String to, OffsetDateTime at) {
    }

    /** The most recent liveness events, newest first. */
    public List<LivenessEventRow> recentLivenessEvents(int limit) {
        return jdbc.sql("SELECT * FROM liveness_events ORDER BY event_id DESC LIMIT :limit")
                .param("limit", limit)
                .query((rs, i) -> new LivenessEventRow(rs.getLong("event_id"), rs.getString("node_id"),
                        rs.getString("from_state"), rs.getString("to_state"), rs.getObject("at", OffsetDateTime.class)))
                .list();
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

    /** createdAt comes from the coordinator's clock: the pending timeout is measured on it. */
    public void insertPendingObject(UUID objectId, String key, long sizeBytes, OffsetDateTime createdAt) {
        jdbc.sql("""
                INSERT INTO objects (object_id, key, size_bytes, status, created_at)
                VALUES (:objectId, :key, :sizeBytes, 'PENDING', :createdAt)
                """)
                .param("objectId", objectId)
                .param("key", key)
                .param("sizeBytes", sizeBytes)
                .param("createdAt", createdAt)
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
                LEFT JOIN chunk_placements p ON p.chunk_id = c.chunk_id AND p.origin = 'initiate'
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

    /** Removes a key, returning the object it pointed to. Locks the keys row first. */
    public Optional<UUID> deleteKey(String key) {
        return jdbc.sql("DELETE FROM keys WHERE key = :key RETURNING object_id")
                .param("key", key).query(UUID.class).optional();
    }

    public boolean isPointedTo(UUID objectId) {
        return jdbc.sql("SELECT EXISTS (SELECT 1 FROM keys WHERE object_id = :id)")
                .param("id", objectId).query(Boolean.class).single();
    }

    /** Moves an object from one status to a retired one. Returns rows changed (0 if it wasn't in {@code from}). */
    public int retire(UUID objectId, Status from, Status to, OffsetDateTime now) {
        return jdbc.sql("""
                UPDATE objects SET status = :to, retired_at = :now
                WHERE object_id = :id AND status = :from
                """)
                .param("to", to.name()).param("now", now).param("id", objectId).param("from", from.name())
                .update();
    }

    /** One key as listed by ls. */
    public record ObjectSummary(String key, UUID objectId, long sizeBytes, OffsetDateTime committedAt) {
    }

    /**
     * Up to limit keys that currently resolve, starting with prefix and sorting
     * after startAfter (exclusive), in key order. The comparison and the order
     * use the same (the column's) collation, which is deterministic, so every
     * key sorts strictly before or after the cursor: no gaps, no repeats.
     */
    public List<ObjectSummary> listKeys(String prefix, String startAfter, int limit) {
        return jdbc.sql("""
                SELECT k.key, o.object_id, o.size_bytes, o.committed_at
                FROM keys k JOIN objects o ON o.object_id = k.object_id
                WHERE starts_with(k.key, :prefix) AND k.key > :after
                ORDER BY k.key
                LIMIT :limit
                """)
                .param("prefix", prefix).param("after", startAfter).param("limit", limit)
                .query((rs, i) -> new ObjectSummary(rs.getString("key"), rs.getObject("object_id", UUID.class),
                        rs.getLong("size_bytes"), rs.getObject("committed_at", OffsetDateTime.class)))
                .list();
    }

    /** A replica row of any state, as stat shows it. */
    public record ReplicaDetail(String nodeId, String state, String origin, String badReason) {
    }

    public record ChunkDetail(UUID chunkId, int index, long sizeBytes, byte[] sha256, List<ReplicaDetail> replicas) {
    }

    public record ObjectDetail(UUID objectId, long sizeBytes, OffsetDateTime committedAt, List<ChunkDetail> chunks) {
    }

    /** The object a key resolves to, with every replica row of every chunk, in one statement. */
    public Optional<ObjectDetail> objectDetailByKey(String key) {
        record Row(UUID objectId, long objectSize, OffsetDateTime committedAt, UUID chunkId, int index, long size,
                byte[] sha, String nodeId, String state, String origin, String badReason) {
        }
        List<Row> rows = jdbc.sql("""
                SELECT o.object_id, o.size_bytes AS object_size, o.committed_at,
                       c.chunk_id, c.chunk_index, c.size_bytes AS chunk_size, c.sha256,
                       r.node_id, r.state, r.origin, r.bad_reason
                FROM keys k
                JOIN objects o ON o.object_id = k.object_id
                LEFT JOIN chunks c ON c.object_id = o.object_id
                LEFT JOIN chunk_replicas r ON r.chunk_id = c.chunk_id
                WHERE k.key = :key
                ORDER BY c.chunk_index, r.node_id
                """)
                .param("key", key)
                .query((rs, i) -> new Row(rs.getObject("object_id", UUID.class), rs.getLong("object_size"),
                        rs.getObject("committed_at", OffsetDateTime.class), rs.getObject("chunk_id", UUID.class),
                        rs.getInt("chunk_index"), rs.getLong("chunk_size"), rs.getBytes("sha256"),
                        rs.getString("node_id"), rs.getString("state"), rs.getString("origin"),
                        rs.getString("bad_reason")))
                .list();
        if (rows.isEmpty()) {
            return Optional.empty();
        }
        Map<UUID, ChunkDetail> chunks = new LinkedHashMap<>();
        for (Row r : rows) {
            if (r.chunkId() == null) {
                continue;
            }
            ChunkDetail c = chunks.computeIfAbsent(r.chunkId(),
                    id -> new ChunkDetail(id, r.index(), r.size(), r.sha(), new ArrayList<>()));
            if (r.nodeId() != null) {
                c.replicas().add(new ReplicaDetail(r.nodeId(), r.state(), r.origin(), r.badReason()));
            }
        }
        Row first = rows.getFirst();
        return Optional.of(new ObjectDetail(first.objectId(), first.objectSize(), first.committedAt(),
                List.copyOf(chunks.values())));
    }

    // --- Garbage collection ---

    /** PENDING objects initiated before cutoff, oldest first. Unlocked: callers re-check under the row lock. */
    public List<UUID> abandonCandidates(OffsetDateTime cutoff, int limit) {
        return jdbc.sql("""
                SELECT object_id FROM objects WHERE status = 'PENDING' AND created_at < :cutoff
                ORDER BY created_at LIMIT :limit
                """)
                .param("cutoff", cutoff).param("limit", limit).query(UUID.class).list();
    }

    /** Whether an object (locked by the caller) was initiated before cutoff. */
    public boolean createdBefore(UUID objectId, OffsetDateTime cutoff) {
        return jdbc.sql("SELECT created_at < :cutoff FROM objects WHERE object_id = :id")
                .param("cutoff", cutoff).param("id", objectId).query(Boolean.class).single();
    }

    /** COMMITTED objects no key points to: superseded by an overwrite. Unlocked: callers re-check. */
    public List<UUID> supersedeCandidates(int limit) {
        return jdbc.sql("""
                SELECT o.object_id FROM objects o
                WHERE o.status = 'COMMITTED'
                  AND NOT EXISTS (SELECT 1 FROM keys k WHERE k.object_id = o.object_id)
                ORDER BY o.committed_at LIMIT :limit
                """)
                .param("limit", limit).query(UUID.class).list();
    }

    /**
     * Retired objects whose files may now be deleted (ABANDONED at once, the
     * others once retired before cutoff), and that still have a placement or
     * replica row without a tombstone task: never scheduled, or something was
     * recorded after scheduling. Running it again finds nothing new.
     */
    public List<UUID> scheduleCandidates(OffsetDateTime cutoff, int limit) {
        return jdbc.sql("""
                SELECT o.object_id FROM objects o
                WHERE o.status IN ('SUPERSEDED', 'DELETED', 'ABANDONED')
                  AND (o.status = 'ABANDONED' OR o.retired_at < :cutoff)
                  AND (o.gc_started_at IS NULL OR EXISTS (
                        SELECT 1 FROM chunks c
                        JOIN (SELECT chunk_id, node_id FROM chunk_placements
                              UNION SELECT chunk_id, node_id FROM chunk_replicas) f ON f.chunk_id = c.chunk_id
                        WHERE c.object_id = o.object_id
                          AND NOT EXISTS (SELECT 1 FROM pending_deletions d
                                          WHERE d.chunk_id = f.chunk_id AND d.node_id = f.node_id AND d.tombstone)))
                ORDER BY o.retired_at LIMIT :limit
                """)
                .param("cutoff", cutoff).param("limit", limit).query(UUID.class).list();
    }

    /**
     * Creates a tombstone deletion task for every (chunk, node) where the
     * object's files may be: every placement (upload plan and repair targets)
     * and every replica row. A BAD-replica task for the same file is upgraded
     * to a tombstone. Returns the tasks created or upgraded.
     */
    public int insertObjectDeletionTasks(UUID objectId, String reason) {
        return jdbc.sql("""
                INSERT INTO pending_deletions (chunk_id, node_id, tombstone, reason)
                SELECT f.chunk_id, f.node_id, true, :reason
                FROM chunks c
                JOIN (SELECT chunk_id, node_id FROM chunk_placements
                      UNION SELECT chunk_id, node_id FROM chunk_replicas) f ON f.chunk_id = c.chunk_id
                WHERE c.object_id = :id
                ON CONFLICT (chunk_id, node_id) DO UPDATE SET tombstone = true, reason = EXCLUDED.reason
                WHERE NOT pending_deletions.tombstone
                """)
                .param("reason", reason).param("id", objectId).update();
    }

    public void markGcStarted(UUID objectId, OffsetDateTime now) {
        jdbc.sql("UPDATE objects SET gc_started_at = coalesce(gc_started_at, :now) WHERE object_id = :id")
                .param("now", now).param("id", objectId).update();
    }

    /** A BAD replica of a COMMITTED object, with the nodes holding HEALTHY rows of the same chunk. */
    public record BadReplica(UUID chunkId, String nodeId, UUID objectId, List<String> healthyNodeIds) {
    }

    /** BAD replicas of COMMITTED objects marked before cutoff that have no deletion task yet. */
    public List<BadReplica> badReplicaCandidates(OffsetDateTime cutoff, int limit) {
        return jdbc.sql("""
                SELECT b.chunk_id, b.node_id, c.object_id,
                       ARRAY(SELECT h.node_id FROM chunk_replicas h
                             WHERE h.chunk_id = b.chunk_id AND h.state = 'HEALTHY' ORDER BY h.node_id) AS healthy
                FROM chunk_replicas b
                JOIN chunks c ON c.chunk_id = b.chunk_id
                JOIN objects o ON o.object_id = c.object_id
                WHERE b.state = 'BAD' AND o.status = 'COMMITTED' AND b.marked_bad_at < :cutoff
                  AND NOT EXISTS (SELECT 1 FROM pending_deletions d WHERE d.chunk_id = b.chunk_id AND d.node_id = b.node_id)
                ORDER BY b.marked_bad_at LIMIT :limit
                """)
                .param("cutoff", cutoff).param("limit", limit)
                .query((rs, i) -> new BadReplica(rs.getObject("chunk_id", UUID.class), rs.getString("node_id"),
                        rs.getObject("object_id", UUID.class),
                        Arrays.asList((String[]) rs.getArray("healthy").getArray())))
                .list();
    }

    /** A deletion task for one BAD file, without a tombstone. Returns rows inserted. */
    public int insertBadReplicaTask(UUID chunkId, String nodeId) {
        return jdbc.sql("""
                INSERT INTO pending_deletions (chunk_id, node_id, tombstone, reason)
                VALUES (:chunkId, :nodeId, false, 'bad-replica')
                ON CONFLICT (chunk_id, node_id) DO NOTHING
                """)
                .param("chunkId", chunkId).param("nodeId", nodeId).update();
    }

    /** One file to delete on one node. */
    public record DeletionTask(UUID chunkId, String nodeId, boolean tombstone) {
    }

    /** Up to limit of a node's tasks, oldest first. */
    public List<DeletionTask> deletionTasks(String nodeId, int limit) {
        return jdbc.sql("""
                SELECT chunk_id, node_id, tombstone FROM pending_deletions WHERE node_id = :node
                ORDER BY created_at, chunk_id LIMIT :limit
                """)
                .param("node", nodeId).param("limit", limit)
                .query((rs, i) -> new DeletionTask(rs.getObject("chunk_id", UUID.class), rs.getString("node_id"),
                        rs.getBoolean("tombstone")))
                .list();
    }

    /**
     * Records that nodeId itself confirmed it no longer holds chunkId: removes
     * the task, that node's replica row and that node's placement. Returns
     * whether a task existed.
     */
    public boolean confirmDeleted(UUID chunkId, String nodeId) {
        int tasks = jdbc.sql("DELETE FROM pending_deletions WHERE chunk_id = :c AND node_id = :n")
                .param("c", chunkId).param("n", nodeId).update();
        if (tasks == 0) {
            return false;
        }
        jdbc.sql("DELETE FROM chunk_replicas WHERE chunk_id = :c AND node_id = :n")
                .param("c", chunkId).param("n", nodeId).update();
        jdbc.sql("DELETE FROM chunk_placements WHERE chunk_id = :c AND node_id = :n")
                .param("c", chunkId).param("n", nodeId).update();
        return true;
    }

    public void recordDeletionFailure(String nodeId, List<UUID> chunkIds, String error) {
        jdbc.sql("""
                UPDATE pending_deletions SET attempts = attempts + 1, last_error = :err
                WHERE node_id = :n AND chunk_id IN (:ids)
                """)
                .param("err", error).param("n", nodeId).param("ids", chunkIds).update();
    }

    /** Retired objects whose files are all confirmed gone: no task, placement or replica row left. */
    public List<UUID> finalizeCandidates(int limit) {
        return jdbc.sql("""
                SELECT o.object_id FROM objects o
                WHERE o.status IN ('SUPERSEDED', 'DELETED', 'ABANDONED') AND o.gc_started_at IS NOT NULL
                  AND NOT EXISTS (
                        SELECT 1 FROM chunks c
                        WHERE c.object_id = o.object_id
                          AND (EXISTS (SELECT 1 FROM pending_deletions d WHERE d.chunk_id = c.chunk_id)
                            OR EXISTS (SELECT 1 FROM chunk_placements p WHERE p.chunk_id = c.chunk_id)
                            OR EXISTS (SELECT 1 FROM chunk_replicas r WHERE r.chunk_id = c.chunk_id)))
                ORDER BY o.retired_at LIMIT :limit
                """)
                .param("limit", limit).query(UUID.class).list();
    }

    /** Whether any task, placement or replica row remains for an object's chunks. */
    public boolean hasFilesLeft(UUID objectId) {
        return jdbc.sql("""
                SELECT EXISTS (
                  SELECT 1 FROM chunks c WHERE c.object_id = :id
                    AND (EXISTS (SELECT 1 FROM pending_deletions d WHERE d.chunk_id = c.chunk_id)
                      OR EXISTS (SELECT 1 FROM chunk_placements p WHERE p.chunk_id = c.chunk_id)
                      OR EXISTS (SELECT 1 FROM chunk_replicas r WHERE r.chunk_id = c.chunk_id)))
                """)
                .param("id", objectId).query(Boolean.class).single();
    }

    /** Deletes an object's chunks rows, then its objects row. */
    public void removeObjectMetadata(UUID objectId) {
        jdbc.sql("DELETE FROM chunks WHERE object_id = :id").param("id", objectId).update();
        jdbc.sql("DELETE FROM objects WHERE object_id = :id").param("id", objectId).update();
    }

    /** A nodes row as GetClusterStatus reports it. */
    public record NodeRow(String nodeId, OffsetDateTime stateChangedAt, OffsetDateTime lastHeartbeatAt) {
    }

    public List<NodeRow> listNodeRows() {
        return jdbc.sql("SELECT node_id, state_changed_at, last_heartbeat_at FROM nodes ORDER BY node_id")
                .query((rs, i) -> new NodeRow(rs.getString("node_id"),
                        rs.getObject("state_changed_at", OffsetDateTime.class),
                        rs.getObject("last_heartbeat_at", OffsetDateTime.class)))
                .list();
    }

    public long countPendingDeletions() {
        return jdbc.sql("SELECT count(*) FROM pending_deletions").query(Long.class).single();
    }

    /** A repair_passes row. */
    public record RepairPassRow(long passId, OffsetDateTime startedAt, OffsetDateTime finishedAt, String skippedReason,
            int underReplicated, int copiesStarted, int replicasRecorded, int foundRecorded, int badFound, int failures) {
    }

    /**
     * The newest repair passes, newest first: with onlyEventful, only those that
     * were skipped or have any non-zero counter.
     */
    public List<RepairPassRow> recentRepairPasses(int limit, boolean onlyEventful) {
        return jdbc.sql("""
                SELECT * FROM repair_passes
                WHERE NOT :eventful OR skipped_reason IS NOT NULL
                   OR under_replicated + copies_started + replicas_recorded + found_recorded + bad_found + failures > 0
                ORDER BY pass_id DESC LIMIT :limit
                """)
                .param("eventful", onlyEventful).param("limit", limit)
                .query((rs, i) -> new RepairPassRow(rs.getLong("pass_id"),
                        rs.getObject("started_at", OffsetDateTime.class), rs.getObject("finished_at", OffsetDateTime.class),
                        rs.getString("skipped_reason"), rs.getInt("under_replicated"), rs.getInt("copies_started"),
                        rs.getInt("replicas_recorded"), rs.getInt("found_recorded"), rs.getInt("bad_found"),
                        rs.getInt("failures")))
                .list();
    }

    /** A gc_passes row. */
    public record GcPassRow(long passId, OffsetDateTime startedAt, OffsetDateTime finishedAt, String skippedReason,
            int abandoned, int superseded, int tasksCreated, int filesDeleted, int alreadyAbsent, int failures,
            int objectsRemoved) {
    }

    /** As {@link #recentRepairPasses}, for garbage collection. */
    public List<GcPassRow> recentGcPasses(int limit, boolean onlyEventful) {
        return jdbc.sql("""
                SELECT * FROM gc_passes
                WHERE NOT :eventful OR skipped_reason IS NOT NULL
                   OR abandoned + superseded + tasks_created + files_deleted + already_absent + failures
                      + objects_removed > 0
                ORDER BY pass_id DESC LIMIT :limit
                """)
                .param("eventful", onlyEventful).param("limit", limit)
                .query((rs, i) -> new GcPassRow(rs.getLong("pass_id"),
                        rs.getObject("started_at", OffsetDateTime.class), rs.getObject("finished_at", OffsetDateTime.class),
                        rs.getString("skipped_reason"), rs.getInt("abandoned"), rs.getInt("superseded"),
                        rs.getInt("tasks_created"), rs.getInt("files_deleted"), rs.getInt("already_absent"),
                        rs.getInt("failures"), rs.getInt("objects_removed")))
                .list();
    }

    public long startGcPass() {
        return jdbc.sql("INSERT INTO gc_passes DEFAULT VALUES RETURNING pass_id").query(Long.class).single();
    }

    public void finishGcPass(long passId, String skipped, int abandoned, int superseded, int tasks, int deleted,
            int absent, int failures, int removed) {
        jdbc.sql("""
                UPDATE gc_passes SET finished_at = clock_timestamp(), skipped_reason = :skipped,
                  abandoned = :abandoned, superseded = :superseded, tasks_created = :tasks,
                  files_deleted = :deleted, already_absent = :absent, failures = :failures, objects_removed = :removed
                WHERE pass_id = :id
                """)
                .param("skipped", skipped).param("abandoned", abandoned).param("superseded", superseded)
                .param("tasks", tasks).param("deleted", deleted).param("absent", absent).param("failures", failures)
                .param("removed", removed).param("id", passId).update();
    }

    public int pruneGcPasses(java.time.Duration retention) {
        return jdbc.sql("DELETE FROM gc_passes WHERE started_at < clock_timestamp() - make_interval(secs => :secs)")
                .param("secs", (double) retention.toMillis() / 1000).update();
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
