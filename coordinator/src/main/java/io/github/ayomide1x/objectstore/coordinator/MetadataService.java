package io.github.ayomide1x.objectstore.coordinator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import io.github.ayomide1x.objectstore.coordinator.MetadataRepository.ObjectRow;
import io.github.ayomide1x.objectstore.coordinator.MetadataRepository.PlannedChunk;
import io.github.ayomide1x.objectstore.v1.ChunkStat;
import io.grpc.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

/** Placement and metadata state transitions. */
@Service
public class MetadataService {

    static final long CHUNK_SIZE = 4L << 20;
    static final long MAX_OBJECT_SIZE = 5L << 30;
    static final int SHA256_LENGTH = 32;
    /** Nodes each chunk is placed on. */
    static final int REPLICATION = 3;
    /** Verified replicas each chunk needs before its object can commit (invariant 2). */
    static final int MIN_REPLICAS = 2;

    private static final Logger log = LoggerFactory.getLogger(MetadataService.class);

    private final MetadataRepository repository;
    private final NodeClient nodeClient;
    private final TransactionTemplate tx;

    public MetadataService(MetadataRepository repository, NodeClient nodeClient, TransactionTemplate tx) {
        this.repository = repository;
        this.nodeClient = nodeClient;
        this.tx = tx;
    }

    /** One chunk of an upload plan, with the nodes the client must write it to. */
    public record ChunkPlacement(UUID chunkId, int index, long sizeBytes, List<Node> nodes) {
    }

    /** The plan for a new upload, as recorded in Postgres. */
    public record UploadPlan(UUID objectId, List<ChunkPlacement> chunks) {
    }

    /** A chunk the client wrote, with the SHA-256 it computed while sending. */
    public record CompletedChunk(UUID chunkId, int index, long sizeBytes, byte[] sha256) {
    }

    /**
     * Records a PENDING object with its full chunk plan and placement. The
     * object is unreachable from any key until it is completed. Whether the
     * key already exists does not matter: completing overwrites it.
     *
     * <p>Placement rotates through the registered nodes (sorted by ID) from a
     * random start per object: chunk i goes to nodes start+i, start+i+1 and
     * start+i+2. Each chunk leaves out one node, and each node holds about
     * (n-1)/n of an object's chunks. There is no liveness until stage 6, so
     * stopped nodes are placed like any other.
     */
    @Transactional
    public UploadPlan initiateUpload(String key, long sizeBytes) {
        requireKey(key);
        if (sizeBytes < 0 || sizeBytes > MAX_OBJECT_SIZE) {
            throw RequestException.invalid(
                    "size_bytes %d outside [0, %d]".formatted(sizeBytes, MAX_OBJECT_SIZE));
        }
        List<Node> nodes = repository.listNodes();
        if (nodes.size() < MIN_REPLICAS) {
            throw new RequestException(Status.Code.UNAVAILABLE,
                    "%d storage nodes registered, need at least %d".formatted(nodes.size(), MIN_REPLICAS));
        }
        int width = Math.min(REPLICATION, nodes.size());
        int start = ThreadLocalRandom.current().nextInt(nodes.size());

        UUID objectId = UUID.randomUUID();
        repository.insertPendingObject(objectId, key, sizeBytes);

        List<ChunkPlacement> chunks = new ArrayList<>();
        int count = chunkCount(sizeBytes);
        for (int index = 0; index < count; index++) {
            long chunkSize = Math.min(CHUNK_SIZE, sizeBytes - index * CHUNK_SIZE);
            UUID chunkId = UUID.randomUUID();
            repository.insertChunk(chunkId, objectId, index, chunkSize);
            List<Node> targets = new ArrayList<>();
            for (int r = 0; r < width; r++) {
                Node node = nodes.get((start + index + r) % nodes.size());
                targets.add(node);
                repository.insertPlacement(chunkId, node.nodeId());
            }
            chunks.add(new ChunkPlacement(chunkId, index, chunkSize, targets));
        }
        return new UploadPlan(objectId, chunks);
    }

    /**
     * Commits an upload, in three phases:
     *
     * <ol>
     *   <li>Validate the request against the plan issued at initiate (reads only).
     *   <li>Ask every placed node, in one batched call each, which of the
     *       object's chunks it durably holds. This runs outside any
     *       transaction, so no row lock is held across the network. A replica
     *       counts only if its node reports a valid header with the planned
     *       size and the same SHA-256 the client reported. Any chunk with fewer
     *       than {@link #MIN_REPLICAS}: FAILED_PRECONDITION, nothing written.
     *   <li>In one transaction: record exactly the verified replicas and the
     *       agreed hash, mark the object COMMITTED, and point the key at it.
     * </ol>
     *
     * The client's own claims about which nodes acknowledged are not used:
     * metadata records only what nodes report (invariant 3). Completing an
     * already COMMITTED object is a successful no-op, so a client may retry
     * after a lost response.
     */
    public void completeUpload(UUID objectId, String key, long sizeBytes, List<CompletedChunk> chunks) {
        ObjectRow object = repository.findObject(objectId)
                .orElseThrow(() -> new RequestException(Status.Code.NOT_FOUND,
                        "object %s not found".formatted(objectId)));
        if (object.status() == MetadataRepository.Status.COMMITTED) {
            log.atInfo().addKeyValue("object_id", objectId).log("complete of committed object is a no-op");
            return;
        }
        // proto3 cannot tell an absent int64 from 0, so both must always match.
        if (!object.key().equals(key)) {
            throw RequestException.invalid("key '%s' does not match upload key '%s'".formatted(key, object.key()));
        }
        if (object.sizeBytes() != sizeBytes) {
            throw RequestException.invalid(
                    "size_bytes %d does not match upload size %d".formatted(sizeBytes, object.sizeBytes()));
        }
        List<PlannedChunk> plan = repository.plannedChunks(objectId);
        validateAgainstPlan(objectId, plan, chunks, sizeBytes);

        Map<UUID, List<String>> verified = verifyReplicas(objectId, plan, chunks);

        tx.executeWithoutResult(status -> commitVerified(objectId, chunks, verified));
    }

    /** Phase 3. Runs inside a transaction. */
    private void commitVerified(UUID objectId, List<CompletedChunk> chunks, Map<UUID, List<String>> verified) {
        ObjectRow object = repository.lockObject(objectId).orElseThrow();
        if (object.status() == MetadataRepository.Status.COMMITTED) {
            // A concurrent complete of the same upload committed first.
            log.atInfo().addKeyValue("object_id", objectId).log("complete of committed object is a no-op");
            return;
        }
        for (CompletedChunk chunk : chunks) {
            repository.recordChunkHash(chunk.chunkId(), chunk.sha256());
            for (String nodeId : verified.get(chunk.chunkId())) {
                repository.insertReplica(chunk.chunkId(), nodeId);
            }
        }
        repository.markCommitted(objectId);
        repository.swapKey(object.key(), objectId);

        for (CompletedChunk chunk : chunks) {
            for (String nodeId : verified.get(chunk.chunkId())) {
                log.atInfo()
                        .addKeyValue("object_id", objectId)
                        .addKeyValue("chunk_id", chunk.chunkId())
                        .addKeyValue("node_id", nodeId)
                        .log("verified replica recorded");
            }
        }
        log.atInfo()
                .addKeyValue("object_id", objectId)
                .addKeyValue("key", object.key())
                .addKeyValue("chunks", chunks.size())
                .log("object committed");
    }

    /**
     * Phase 2. Returns, per chunk, the placed nodes that verifiably hold it,
     * or throws FAILED_PRECONDITION if any chunk has fewer than MIN_REPLICAS.
     */
    private Map<UUID, List<String>> verifyReplicas(UUID objectId, List<PlannedChunk> plan,
            List<CompletedChunk> chunks) {
        Map<String, Node> nodesById = new HashMap<>();
        for (Node node : repository.listNodes()) {
            nodesById.put(node.nodeId(), node);
        }
        Map<Node, List<UUID>> request = new LinkedHashMap<>();
        for (PlannedChunk p : plan) {
            for (String nodeId : p.placedNodeIds()) {
                request.computeIfAbsent(nodesById.get(nodeId), n -> new ArrayList<>()).add(p.chunkId());
            }
        }
        Map<String, Map<UUID, ChunkStat>> stats = nodeClient.statChunks(objectId, request);

        Map<UUID, PlannedChunk> planned = new HashMap<>();
        for (PlannedChunk p : plan) {
            planned.put(p.chunkId(), p);
        }
        Map<UUID, List<String>> verified = new HashMap<>();
        for (CompletedChunk chunk : chunks) {
            PlannedChunk p = planned.get(chunk.chunkId());
            List<String> ok = new ArrayList<>();
            List<String> rejected = new ArrayList<>();
            for (String nodeId : p.placedNodeIds()) {
                Map<UUID, ChunkStat> nodeStats = stats.get(nodeId);
                ChunkStat s = nodeStats == null ? null : nodeStats.get(chunk.chunkId());
                String why = rejection(s, p, chunk);
                if (why == null) {
                    ok.add(nodeId);
                } else {
                    rejected.add(nodeId + ": " + why);
                    if (s != null && s.getState() == ChunkStat.State.STATE_FOUND) {
                        // Found, but not what the client sent: a client bug or a node bug.
                        log.atError()
                                .addKeyValue("object_id", objectId)
                                .addKeyValue("chunk_id", chunk.chunkId())
                                .addKeyValue("node_id", nodeId)
                                .log("replica does not match the upload: {}", why);
                    }
                }
            }
            if (ok.size() < MIN_REPLICAS) {
                throw new RequestException(Status.Code.FAILED_PRECONDITION,
                        "chunk %s (index %d) has %d verified replicas, need %d: %s".formatted(chunk.chunkId(),
                                chunk.index(), ok.size(), MIN_REPLICAS, String.join("; ", rejected)));
            }
            verified.put(chunk.chunkId(), ok);
        }
        return verified;
    }

    /** Why a node's report does not count as a replica of chunk, or null if it does. */
    private static String rejection(ChunkStat s, PlannedChunk p, CompletedChunk chunk) {
        if (s == null) {
            return "no answer";
        }
        return switch (s.getState()) {
            case STATE_MISSING -> "missing";
            case STATE_BAD_HEADER -> "bad header";
            case STATE_FOUND -> {
                if (s.getSizeBytes() != p.sizeBytes()) {
                    yield "holds %d bytes, planned %d".formatted(s.getSizeBytes(), p.sizeBytes());
                }
                if (!Arrays.equals(s.getSha256().toByteArray(), chunk.sha256())) {
                    yield "sha256 differs from the client's";
                }
                yield null;
            }
            default -> "unknown state " + s.getState();
        };
    }

    @Transactional(readOnly = true)
    public Optional<StoredObject> getObject(String key) {
        requireKey(key);
        return repository.findObjectByKey(key);
    }

    static int chunkCount(long sizeBytes) {
        return Math.toIntExact((sizeBytes + CHUNK_SIZE - 1) / CHUNK_SIZE);
    }

    /**
     * Requires the request to describe exactly the issued plan: the same chunk
     * IDs, each with its allocated index and size, and a 32-byte SHA-256.
     */
    private static void validateAgainstPlan(UUID objectId, List<PlannedChunk> plan, List<CompletedChunk> chunks,
            long sizeBytes) {
        if (chunks.size() != plan.size()) {
            throw RequestException.invalid(
                    "object %s has %d chunks, request has %d".formatted(objectId, plan.size(), chunks.size()));
        }
        Map<UUID, PlannedChunk> planned = new HashMap<>();
        for (PlannedChunk p : plan) {
            planned.put(p.chunkId(), p);
        }

        Set<UUID> seen = new HashSet<>();
        long total = 0;
        for (CompletedChunk chunk : chunks) {
            PlannedChunk p = planned.get(chunk.chunkId());
            if (p == null) {
                throw RequestException.invalid(
                        "chunk %s was not allocated to object %s".formatted(chunk.chunkId(), objectId));
            }
            if (!seen.add(chunk.chunkId())) {
                throw RequestException.invalid("chunk %s listed twice".formatted(chunk.chunkId()));
            }
            if (chunk.index() != p.index()) {
                throw RequestException.invalid("chunk %s has index %d, allocated %d"
                        .formatted(chunk.chunkId(), chunk.index(), p.index()));
            }
            if (chunk.sizeBytes() != p.sizeBytes()) {
                throw RequestException.invalid("chunk %s has size %d, allocated %d"
                        .formatted(chunk.chunkId(), chunk.sizeBytes(), p.sizeBytes()));
            }
            int shaLength = chunk.sha256() == null ? 0 : chunk.sha256().length;
            if (shaLength != SHA256_LENGTH) {
                throw RequestException.invalid("chunk %s sha256 is %d bytes, want %d"
                        .formatted(chunk.chunkId(), shaLength, SHA256_LENGTH));
            }
            total += chunk.sizeBytes();
        }
        // Implied by the checks above, since allocated sizes sum to the object
        // size; kept as a direct statement of the invariant.
        if (total != sizeBytes) {
            throw RequestException.invalid("chunk sizes sum to %d, object size is %d".formatted(total, sizeBytes));
        }
    }

    private static void requireKey(String key) {
        if (key == null || key.isEmpty()) {
            throw RequestException.invalid("key is required");
        }
    }
}
