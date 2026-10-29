package io.github.ayomide1x.objectstore.coordinator;

import java.util.Optional;
import java.util.UUID;

import io.github.ayomide1x.objectstore.v1.ReportBadReplicaResponse.Verdict;
import io.github.ayomide1x.objectstore.v1.VerifyChunkResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Decides whether a replica is bad. A client's report is only a hint: the
 * replica is marked BAD only when its own node, asked directly, confirms the
 * chunk is corrupt or missing (invariant 3: facts about a node are recorded
 * only from a response that identifies as that node).
 */
@Service
public class ReplicaHealthService {

    private static final Logger log = LoggerFactory.getLogger(ReplicaHealthService.class);

    private final MetadataRepository repository;
    private final NodeClient nodeClient;

    public ReplicaHealthService(MetadataRepository repository, NodeClient nodeClient) {
        this.repository = repository;
        this.nodeClient = nodeClient;
    }

    /**
     * Handles a client's report that a replica served bad data or none. The
     * node is asked to re-hash the chunk against the recorded SHA-256 outside
     * any transaction; only a confirmed CORRUPT or MISSING marks the replica.
     */
    public Verdict reportBadReplica(UUID chunkId, String nodeId, String reason, String detail) {
        var logCtx = log.atInfo().addKeyValue("chunk_id", chunkId).addKeyValue("node_id", nodeId)
                .addKeyValue("reported_reason", reason);
        Optional<String> state = repository.replicaState(chunkId, nodeId);
        if (state.isEmpty()) {
            logCtx.log("bad replica report for a replica that is not recorded");
            return Verdict.VERDICT_NOT_A_REPLICA;
        }
        if (state.get().equals("BAD")) {
            return Verdict.VERDICT_ALREADY_BAD;
        }
        Optional<Node> node = repository.listNodes().stream().filter(n -> n.nodeId().equals(nodeId)).findFirst();
        Optional<byte[]> expected = repository.chunkHash(chunkId);
        if (node.isEmpty() || expected.isEmpty()) {
            return Verdict.VERDICT_NOT_A_REPLICA;
        }

        Optional<VerifyChunkResponse> answer = nodeClient.verifyChunk(node.get(), chunkId, expected.get());
        if (answer.isEmpty()) {
            logCtx.log("bad replica report unconfirmed: the node did not answer as itself");
            return Verdict.VERDICT_UNCONFIRMED;
        }
        String markReason = switch (answer.get().getState()) {
            case STATE_CORRUPT -> "corrupt: " + answer.get().getDetail();
            case STATE_MISSING -> "missing";
            case STATE_HEALTHY -> null;
            default -> null;
        };
        if (markReason == null) {
            log.atWarn().addKeyValue("chunk_id", chunkId).addKeyValue("node_id", nodeId)
                    .addKeyValue("reported_reason", reason).addKeyValue("node_state", answer.get().getState())
                    .log("bad replica report contradicted by the node; nothing marked (client saw: {})", detail);
            return Verdict.VERDICT_CONTRADICTED;
        }
        if (repository.markReplicaBad(chunkId, nodeId, markReason) == 0) {
            return Verdict.VERDICT_ALREADY_BAD;
        }
        log.atWarn().addKeyValue("chunk_id", chunkId).addKeyValue("node_id", nodeId)
                .addKeyValue("reported_reason", reason)
                .log("replica marked BAD, confirmed by its node: {}", markReason);
        return Verdict.VERDICT_MARKED_BAD;
    }
}
