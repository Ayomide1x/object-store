package io.github.ayomide1x.objectstore.coordinator;

import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import com.google.protobuf.ByteString;

import io.github.ayomide1x.objectstore.v1.ChunkStat;
import io.github.ayomide1x.objectstore.v1.PushChunkRequest;
import io.github.ayomide1x.objectstore.v1.PushChunkResponse;
import io.github.ayomide1x.objectstore.v1.StatChunksRequest;
import io.github.ayomide1x.objectstore.v1.StatChunksResponse;
import io.github.ayomide1x.objectstore.v1.StorageNodeServiceGrpc;
import io.github.ayomide1x.objectstore.v1.VerifyChunkRequest;
import io.github.ayomide1x.objectstore.v1.VerifyChunkResponse;
import io.grpc.Deadline;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.stereotype.Component;

/**
 * Calls storage nodes on their internal addresses. Channels are cached per
 * address; they are only connections, rebuilt on demand (invariant 7).
 *
 * <p>Two rules apply to every node response the coordinator records facts
 * from:
 * <ul>
 *   <li>It must identify as the node that was called. A node's address can go
 *       stale (container IPs change on restart), so a call can reach a
 *       different node; a response naming another node is logged and treated
 *       as no answer, so one node's chunks are never credited to another
 *       (invariant 3).
 *   <li>On UNAVAILABLE, the channel is replaced (so the address is looked up
 *       again) and the call retried once, within the same deadline.
 * </ul>
 */
@Component
public class NodeClient implements DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(NodeClient.class);

    private final Duration timeout;
    private final Duration copyTimeout;
    private final Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();

    public NodeClient(CoordinatorProperties properties) {
        this.timeout = properties.nodeRpcTimeout();
        this.copyTimeout = properties.copyTimeout();
    }

    /**
     * Orders source to copy a chunk to target (PushChunk), within the copy
     * deadline. Throws the source's status on a problem with its own copy.
     * Nothing about the target may be recorded from this answer: callers
     * confirm with the target's own StatChunks.
     */
    public PushChunkResponse pushChunk(Node source, UUID chunkId, byte[] expectedSha256, Node target) {
        Deadline deadline = Deadline.after(copyTimeout.toMillis(), TimeUnit.MILLISECONDS);
        PushChunkRequest req = PushChunkRequest.newBuilder()
                .setChunkId(chunkId.toString())
                .setExpectedSha256(ByteString.copyFrom(expectedSha256))
                .setTargetNodeId(target.nodeId())
                .setTargetAddress(target.internalAddr())
                .build();
        return StorageNodeServiceGrpc.newBlockingStub(channel(source)).withDeadline(deadline).pushChunk(req);
    }

    /**
     * Sends one StatChunks call per node, all in parallel, each bounded by the
     * node RPC deadline, so a hung node costs one deadline whatever the object
     * size. Returns each answering node's stats by chunk ID. A node that fails,
     * misses the deadline, or answers as another node is absent: none of its
     * replicas count.
     */
    public Map<String, Map<UUID, ChunkStat>> statChunks(UUID objectId, Map<Node, List<UUID>> request) {
        Deadline deadline = Deadline.after(timeout.toMillis(), TimeUnit.MILLISECONDS);
        Map<String, Map<UUID, ChunkStat>> result = new HashMap<>();
        try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
            Map<Node, Future<StatChunksResponse>> calls = new LinkedHashMap<>();
            for (Map.Entry<Node, List<UUID>> e : request.entrySet()) {
                StatChunksRequest req = StatChunksRequest.newBuilder()
                        .addAllChunkIds(e.getValue().stream().map(UUID::toString).toList())
                        .build();
                calls.put(e.getKey(), pool.submit(
                        () -> callWithRetry(e.getKey(), deadline, "StatChunks", stub -> stub.statChunks(req))));
            }
            for (Map.Entry<Node, Future<StatChunksResponse>> call : calls.entrySet()) {
                Node node = call.getKey();
                try {
                    StatChunksResponse resp = call.getValue().get();
                    if (!node.nodeId().equals(resp.getNodeId())) {
                        log.atError()
                                .addKeyValue("object_id", objectId)
                                .addKeyValue("node_id", node.nodeId())
                                .addKeyValue("responder_node_id", resp.getNodeId())
                                .addKeyValue("addr", node.internalAddr())
                                .log("StatChunks answered by the wrong node; treated as no answer");
                        continue;
                    }
                    Map<UUID, ChunkStat> byChunk = new HashMap<>();
                    for (ChunkStat s : resp.getStatsList()) {
                        byChunk.put(UUID.fromString(s.getChunkId()), s);
                    }
                    result.put(node.nodeId(), byChunk);
                } catch (ExecutionException e) {
                    log.atWarn()
                            .addKeyValue("object_id", objectId)
                            .addKeyValue("node_id", node.nodeId())
                            .log("StatChunks failed; none of this node's replicas count: {}", e.getCause().toString());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("interrupted while asking nodes", e);
                }
            }
        }
        return result;
    }

    /**
     * Asks one node to re-read and re-hash a chunk (VerifyChunk), within one
     * node RPC deadline. Empty if the node doesn't answer, or if the answer
     * comes from another node: either way nothing can be concluded.
     */
    public Optional<VerifyChunkResponse> verifyChunk(Node node, UUID chunkId, byte[] expectedSha256) {
        Deadline deadline = Deadline.after(timeout.toMillis(), TimeUnit.MILLISECONDS);
        VerifyChunkRequest req = VerifyChunkRequest.newBuilder()
                .setChunkId(chunkId.toString())
                .setExpectedSha256(ByteString.copyFrom(expectedSha256))
                .build();
        VerifyChunkResponse answer;
        try {
            answer = callWithRetry(node, deadline, "VerifyChunk", stub -> stub.verifyChunk(req));
        } catch (StatusRuntimeException e) {
            log.atWarn()
                    .addKeyValue("chunk_id", chunkId)
                    .addKeyValue("node_id", node.nodeId())
                    .log("VerifyChunk failed; nothing can be concluded: {}", e.getStatus());
            return Optional.empty();
        }
        if (!node.nodeId().equals(answer.getNodeId())) {
            log.atError()
                    .addKeyValue("chunk_id", chunkId)
                    .addKeyValue("node_id", node.nodeId())
                    .addKeyValue("responder_node_id", answer.getNodeId())
                    .addKeyValue("addr", node.internalAddr())
                    .log("VerifyChunk answered by the wrong node; treated as no answer");
            return Optional.empty();
        }
        return Optional.of(answer);
    }

    private <T> T callWithRetry(Node node, Deadline deadline, String rpc,
            Function<StorageNodeServiceGrpc.StorageNodeServiceBlockingStub, T> call) {
        try {
            return call.apply(stub(node, deadline));
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() != Status.Code.UNAVAILABLE || deadline.isExpired()) {
                throw e;
            }
            // The cached channel may hold a stale address or a dead connection.
            // Replace it, so the address is resolved again, and retry once.
            log.atInfo()
                    .addKeyValue("node_id", node.nodeId())
                    .addKeyValue("addr", node.internalAddr())
                    .log("{} UNAVAILABLE; resetting channel and retrying once: {}", rpc, e.getStatus());
            resetChannel(node);
            return call.apply(stub(node, deadline));
        }
    }

    private StorageNodeServiceGrpc.StorageNodeServiceBlockingStub stub(Node node, Deadline deadline) {
        return StorageNodeServiceGrpc.newBlockingStub(channel(node)).withDeadline(deadline);
    }

    private ManagedChannel channel(Node node) {
        return channels.computeIfAbsent(node.internalAddr(),
                addr -> ManagedChannelBuilder.forTarget(addr).usePlaintext().build());
    }

    private void resetChannel(Node node) {
        ManagedChannel old = channels.remove(node.internalAddr());
        if (old != null) {
            old.shutdownNow();
        }
    }

    @Override
    public void destroy() {
        channels.values().forEach(ManagedChannel::shutdownNow);
    }
}
