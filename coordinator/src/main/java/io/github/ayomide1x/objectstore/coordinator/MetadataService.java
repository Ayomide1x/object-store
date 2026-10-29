package io.github.ayomide1x.objectstore.coordinator;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import io.grpc.Status;
import org.postgresql.util.PSQLException;
import org.postgresql.util.ServerErrorMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/** Placement and metadata state transitions. */
@Service
public class MetadataService {

    static final long MAX_CHUNK_SIZE = 4L << 20;

    private static final Logger log = LoggerFactory.getLogger(MetadataService.class);
    private static final String KEY_CONSTRAINT = "objects_key_unique";

    private final MetadataRepository repository;

    public MetadataService(MetadataRepository repository) {
        this.repository = repository;
    }

    /** IDs and target nodes for a new upload. Nothing is persisted yet. */
    public record Placement(UUID objectId, UUID chunkId, List<Node> nodes) {
    }

    /** A chunk the client wrote, with the nodes that acknowledged it. */
    public record CompletedChunk(UUID chunkId, int index, long sizeBytes, List<String> ackedNodeIds) {
    }

    /**
     * Allocates IDs and picks a node. Deliberately does not check whether the
     * key exists: that check would race with concurrent puts. The unique
     * constraint enforces it at completion instead.
     */
    public Placement initiateUpload(String key, long sizeBytes) {
        requireKey(key);
        if (sizeBytes < 0 || sizeBytes > MAX_CHUNK_SIZE) {
            throw RequestException.invalid(
                    "size_bytes %d outside [0, %d]; stage 1 stores one chunk".formatted(sizeBytes, MAX_CHUNK_SIZE));
        }
        List<Node> nodes = repository.listNodes();
        if (nodes.isEmpty()) {
            throw new RequestException(Status.Code.UNAVAILABLE, "no storage nodes registered");
        }
        return new Placement(UUID.randomUUID(), UUID.randomUUID(), List.of(nodes.getFirst()));
    }

    /**
     * Records the object, its chunk and the acknowledged replicas in one
     * transaction. Any failure, including a duplicate key, rolls back all of it.
     */
    @Transactional
    public void completeUpload(UUID objectId, String key, long sizeBytes, List<CompletedChunk> chunks) {
        requireKey(key);
        if (chunks.size() != 1) {
            throw RequestException.invalid("stage 1 objects have exactly one chunk, got " + chunks.size());
        }
        CompletedChunk chunk = chunks.getFirst();
        if (chunk.index() != 0) {
            throw RequestException.invalid("chunk index must be 0, got " + chunk.index());
        }
        if (chunk.sizeBytes() != sizeBytes || sizeBytes < 0 || sizeBytes > MAX_CHUNK_SIZE) {
            throw RequestException.invalid(
                    "chunk size %d does not match object size %d".formatted(chunk.sizeBytes(), sizeBytes));
        }
        if (chunk.ackedNodeIds().isEmpty()) {
            throw RequestException.invalid("chunk %s has no acknowledged replicas".formatted(chunk.chunkId()));
        }

        try {
            repository.insertObject(objectId, key, sizeBytes);
            repository.insertChunk(chunk.chunkId(), objectId, chunk.index(), chunk.sizeBytes());
            for (String nodeId : chunk.ackedNodeIds()) {
                repository.insertReplica(chunk.chunkId(), nodeId);
            }
        } catch (DuplicateKeyException e) {
            if (KEY_CONSTRAINT.equals(constraintName(e))) {
                throw new RequestException(Status.Code.ALREADY_EXISTS, "key '%s' already exists".formatted(key));
            }
            throw new RequestException(Status.Code.ALREADY_EXISTS,
                    "object or chunk id already recorded (%s)".formatted(constraintName(e)));
        } catch (DataIntegrityViolationException e) {
            throw RequestException.invalid("upload references unknown data (%s)".formatted(constraintName(e)));
        }

        for (String nodeId : chunk.ackedNodeIds()) {
            log.atInfo()
                    .addKeyValue("object_id", objectId)
                    .addKeyValue("chunk_id", chunk.chunkId())
                    .addKeyValue("node_id", nodeId)
                    .addKeyValue("key", key)
                    .log("replica recorded");
        }
    }

    @Transactional(readOnly = true)
    public Optional<StoredObject> getObject(String key) {
        requireKey(key);
        return repository.findObjectByKey(key);
    }

    private static void requireKey(String key) {
        if (key == null || key.isEmpty()) {
            throw RequestException.invalid("key is required");
        }
    }

    /** The Postgres constraint behind an integrity violation, or null. */
    private static String constraintName(Throwable e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof PSQLException psql) {
                ServerErrorMessage msg = psql.getServerErrorMessage();
                return msg == null ? null : msg.getConstraint();
            }
        }
        return null;
    }
}
