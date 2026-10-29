package io.github.ayomide1x.objectstore.coordinator;

import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import io.github.ayomide1x.objectstore.v1.ChunkStat;
import io.github.ayomide1x.objectstore.v1.StatChunksRequest;
import io.github.ayomide1x.objectstore.v1.StatChunksResponse;
import io.github.ayomide1x.objectstore.v1.StorageNodeServiceGrpc;
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
    private final Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();

    public NodeClient(CoordinatorProperties properties) {
        this.timeout = properties.nodeRpcTimeout();
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
                calls.put(e.getKey(), pool.submit(() -> callWithRetry(e.getKey(), req, deadline)));
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

    private StatChunksResponse callWithRetry(Node node, StatChunksRequest req, Deadline deadline) {
        try {
            return stub(node, deadline).statChunks(req);
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() != Status.Code.UNAVAILABLE || deadline.isExpired()) {
                throw e;
            }
            // The cached channel may hold a stale address or a dead connection.
            // Replace it, so the address is resolved again, and retry once.
            log.atInfo()
                    .addKeyValue("node_id", node.nodeId())
                    .addKeyValue("addr", node.internalAddr())
                    .log("StatChunks UNAVAILABLE; resetting channel and retrying once: {}", e.getStatus());
            resetChannel(node);
            return stub(node, deadline).statChunks(req);
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
