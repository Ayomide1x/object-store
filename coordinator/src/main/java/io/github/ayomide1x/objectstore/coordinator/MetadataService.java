package io.github.ayomide1x.objectstore.coordinator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import io.github.ayomide1x.objectstore.coordinator.MetadataRepository.ObjectRow;
import io.github.ayomide1x.objectstore.coordinator.MetadataRepository.PlannedChunk;
import io.grpc.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/** Placement and metadata state transitions. */
@Service
public class MetadataService {

    static final long CHUNK_SIZE = 4L << 20;
    static final long MAX_OBJECT_SIZE = 5L << 30;

    private static final Logger log = LoggerFactory.getLogger(MetadataService.class);

    private final MetadataRepository repository;

    public MetadataService(MetadataRepository repository) {
        this.repository = repository;
    }

    /** One chunk of an upload plan, with the nodes the client must write it to. */
    public record ChunkPlacement(UUID chunkId, int index, long sizeBytes, List<Node> nodes) {
    }

    /** The plan for a new upload, as recorded in Postgres. */
    public record UploadPlan(UUID objectId, List<ChunkPlacement> chunks) {
    }

    /** A chunk the client wrote, with the nodes that acknowledged it. */
    public record CompletedChunk(UUID chunkId, int index, long sizeBytes, List<String> ackedNodeIds) {
    }

    /**
     * Records a PENDING object with its full chunk plan and placement. The
     * object is unreachable from any key until it is completed. Whether the
     * key already exists does not matter: completing overwrites it.
     */
    @Transactional
    public UploadPlan initiateUpload(String key, long sizeBytes) {
        requireKey(key);
        if (sizeBytes < 0 || sizeBytes > MAX_OBJECT_SIZE) {
            throw RequestException.invalid(
                    "size_bytes %d outside [0, %d]".formatted(sizeBytes, MAX_OBJECT_SIZE));
        }
        List<Node> nodes = repository.listNodes();
        if (nodes.isEmpty()) {
            throw new RequestException(Status.Code.UNAVAILABLE, "no storage nodes registered");
        }
        List<Node> targets = List.of(nodes.getFirst());

        UUID objectId = UUID.randomUUID();
        repository.insertPendingObject(objectId, key, sizeBytes);

        List<ChunkPlacement> chunks = new ArrayList<>();
        int count = chunkCount(sizeBytes);
        for (int index = 0; index < count; index++) {
            long chunkSize = Math.min(CHUNK_SIZE, sizeBytes - index * CHUNK_SIZE);
            UUID chunkId = UUID.randomUUID();
            repository.insertChunk(chunkId, objectId, index, chunkSize);
            for (Node node : targets) {
                repository.insertPlacement(chunkId, node.nodeId());
            }
            chunks.add(new ChunkPlacement(chunkId, index, chunkSize, targets));
        }
        return new UploadPlan(objectId, chunks);
    }

    /**
     * Validates the request against the plan issued at initiate, then records
     * the acknowledged replicas, marks the object COMMITTED and points the key
     * at it, all in one transaction. Completing an already COMMITTED object is
     * a successful no-op, so a client may retry after a lost response.
     */
    @Transactional
    public void completeUpload(UUID objectId, String key, long sizeBytes, List<CompletedChunk> chunks) {
        ObjectRow object = repository.lockObject(objectId)
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
        validateAgainstPlan(objectId, repository.plannedChunks(objectId), chunks, sizeBytes);

        for (CompletedChunk chunk : chunks) {
            for (String nodeId : chunk.ackedNodeIds()) {
                repository.insertReplica(chunk.chunkId(), nodeId);
            }
        }
        repository.markCommitted(objectId);
        repository.swapKey(object.key(), objectId);

        for (CompletedChunk chunk : chunks) {
            for (String nodeId : chunk.ackedNodeIds()) {
                log.atInfo()
                        .addKeyValue("object_id", objectId)
                        .addKeyValue("chunk_id", chunk.chunkId())
                        .addKeyValue("node_id", nodeId)
                        .log("replica recorded");
            }
        }
        log.atInfo()
                .addKeyValue("object_id", objectId)
                .addKeyValue("key", object.key())
                .addKeyValue("chunks", chunks.size())
                .log("object committed");
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
     * IDs, each with its allocated index and size, and acknowledgements only
     * from nodes the chunk was placed on.
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
            if (chunk.ackedNodeIds().isEmpty()) {
                throw RequestException.invalid("chunk %s has no acknowledged replicas".formatted(chunk.chunkId()));
            }
            if (new HashSet<>(chunk.ackedNodeIds()).size() != chunk.ackedNodeIds().size()) {
                throw RequestException.invalid("chunk %s lists a node twice".formatted(chunk.chunkId()));
            }
            for (String nodeId : chunk.ackedNodeIds()) {
                if (!p.placedNodeIds().contains(nodeId)) {
                    throw RequestException.invalid(
                            "node %s was not a placement target for chunk %s".formatted(nodeId, chunk.chunkId()));
                }
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
