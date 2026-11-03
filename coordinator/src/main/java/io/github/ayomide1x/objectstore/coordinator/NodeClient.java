package io.github.ayomide1x.objectstore.coordinator;

import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import com.google.protobuf.ByteString;

import io.github.ayomide1x.objectstore.v1.ChunkDeletion;
import io.github.ayomide1x.objectstore.v1.ChunkStat;
import io.github.ayomide1x.objectstore.v1.DeleteChunksRequest;
import io.github.ayomide1x.objectstore.v1.DeleteChunksResponse;
import io.github.ayomide1x.objectstore.v1.PushChunkRequest;
import io.github.ayomide1x.objectstore.v1.PushChunkResponse;
import io.github.ayomide1x.objectstore.v1.StatChunksRequest;
import io.github.ayomide1x.objectstore.v1.StatChunksResponse;
import io.github.ayomide1x.objectstore.v1.StorageNodeServiceGrpc;
import io.github.ayomide1x.objectstore.v1.VerifyChunkRequest;
import io.github.ayomide1x.objectstore.v1.VerifyChunkResponse;
import io.grpc.ConnectivityState;
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
 * <p>A channel can get stuck: not READY, yet never failing, so calls wait out
 * their deadline (DEADLINE_EXCEEDED, never UNAVAILABLE) on a connection that
 * will never come. grpc-java's pick_first can do this when a node's address
 * changes during a reconnect (grpc-java issue 12395), and a peer that accepts
 * TCP but never speaks gRPC does it too. So before every call the channel
 * gets half the call's deadline to be READY; if it isn't, it is replaced (a
 * new channel resolves the address afresh) and the call runs on the new one
 * with the rest of its deadline. That replacement is rate-limited per node
 * ({@code COORDINATOR_NODE_CHANNEL_REPLACE_INTERVAL}). A call that fails
 * UNAVAILABLE, a definite and cheap signal, always gets its replacement (and
 * one retry), as since stage 4: a refusing node's churn is bounded by DEAD
 * detection, since calls to DEAD nodes stop, and a node that restarted on a
 * new address is reached by the first call after it returns. Every
 * replacement is single-flight per node (concurrent calls that find the same
 * stale channel replace it once and all use the new one), logged at WARN and
 * counted; the old channel is shut down, then forced down after a grace.
 *
 * <p>A connection can also go half-open: the node or the path is gone, nothing
 * is closed, and the channel stays READY while every call times out (until
 * TCP gives up, about 15 min). Keepalive pings find it: every channel pings
 * after {@link #KEEPALIVE_TIME} without reading anything, calls or no calls,
 * and closes the connection if no ack comes within {@link #KEEPALIVE_TIMEOUT}.
 * Calls in flight then end UNAVAILABLE (the unlimited replace-and-retry
 * above); with none in flight the channel goes IDLE and the next call's
 * readiness wait reconnects it. Nodes permit pings every 5 s without calls
 * (node/internal/server), half this rate.
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

    /** How long failed calls count toward a node's recent_call_failures. */
    static final Duration FAILURE_WINDOW = Duration.ofMinutes(1);

    /**
     * Keepalive: ping after this long without reading anything. 10 s is
     * grpc-java's floor (shorter values are rounded up to it).
     */
    static final Duration KEEPALIVE_TIME = Duration.ofSeconds(10);
    /** Keepalive: close the connection if a ping isn't acked this fast; the node RPC deadline. */
    static final Duration KEEPALIVE_TIMEOUT = Duration.ofSeconds(2);

    private final Duration timeout;
    private final Duration copyTimeout;
    private final long replaceIntervalNanos;
    private final Map<String, NodeChannel> channels = new ConcurrentHashMap<>();
    /** Every channel created and not yet terminated, replaced ones included. */
    private final Set<ManagedChannel> live = ConcurrentHashMap.newKeySet();
    /** Channels ever created. For tests. */
    private final java.util.concurrent.atomic.AtomicLong created = new java.util.concurrent.atomic.AtomicLong();
    private final ScheduledExecutorService reaper = Executors.newSingleThreadScheduledExecutor(
            r -> Thread.ofPlatform().daemon().name("node-channel-reaper").unstarted(r));

    /** Per-node channel statistics, for GetClusterStatus. */
    public record ChannelStats(long replacements, int recentFailures) {
    }

    /** One node address's channel, and what has happened to it. */
    private final class NodeChannel {
        final String addr;
        volatile ManagedChannel channel;
        // Guarded by this.
        long lastNotReadyReplacementNanos = Long.MIN_VALUE;
        long replacements;
        long notReadyReplacements; // for tests
        final Deque<Long> failures = new ArrayDeque<>();

        NodeChannel(String addr) {
            this.addr = addr;
            this.channel = newChannel(addr);
        }
    }

    public NodeClient(CoordinatorProperties properties) {
        this.timeout = properties.nodeRpcTimeout();
        this.copyTimeout = properties.copyTimeout();
        this.replaceIntervalNanos = properties.nodeChannelReplaceInterval().toNanos();
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
        return call(source, deadline, "PushChunk", stub -> stub.pushChunk(req), false);
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

    /** A node's answer to DeleteChunks: the response, or why there is none to act on. */
    public record DeleteAnswer(DeleteChunksResponse response, String error) {
    }

    /**
     * Asks a node to delete chunk files (DeleteChunks), within one node RPC
     * deadline. The answer counts only if it identifies as that node; a
     * failure or an answer from another node comes back as an error, and
     * nothing may be concluded from it.
     */
    public DeleteAnswer deleteChunks(Node node, List<MetadataRepository.DeletionTask> tasks) {
        Deadline deadline = Deadline.after(timeout.toMillis(), TimeUnit.MILLISECONDS);
        DeleteChunksRequest.Builder req = DeleteChunksRequest.newBuilder().setNodeId(node.nodeId());
        for (MetadataRepository.DeletionTask t : tasks) {
            req.addChunks(ChunkDeletion.newBuilder().setChunkId(t.chunkId().toString()).setTombstone(t.tombstone()));
        }
        DeleteChunksResponse answer;
        try {
            answer = callWithRetry(node, deadline, "DeleteChunks", stub -> stub.deleteChunks(req.build()));
        } catch (StatusRuntimeException e) {
            return new DeleteAnswer(null, "DeleteChunks failed: " + e.getStatus());
        }
        if (!node.nodeId().equals(answer.getNodeId())) {
            log.atError()
                    .addKeyValue("node_id", node.nodeId())
                    .addKeyValue("responder_node_id", answer.getNodeId())
                    .addKeyValue("addr", node.internalAddr())
                    .log("DeleteChunks answered by the wrong node; treated as no answer");
            return new DeleteAnswer(null, "answered by node '%s'".formatted(answer.getNodeId()));
        }
        return new DeleteAnswer(answer, null);
    }

    private <T> T callWithRetry(Node node, Deadline deadline, String rpc,
            Function<StorageNodeServiceGrpc.StorageNodeServiceBlockingStub, T> call) {
        return call(node, deadline, rpc, call, true);
    }

    /**
     * Runs one call to node. First the channel gets half the remaining
     * deadline to be READY, and is replaced if it isn't. Then the call runs;
     * on UNAVAILABLE (with retry), the channel is replaced and the call
     * retried once, within the same deadline. Failures are counted.
     */
    private <T> T call(Node node, Deadline deadline, String rpc,
            Function<StorageNodeServiceGrpc.StorageNodeServiceBlockingStub, T> call, boolean retryUnavailable) {
        NodeChannel nc = channels.computeIfAbsent(node.internalAddr(), NodeChannel::new);
        ManagedChannel ch = ready(node, nc, deadline);
        try {
            return call.apply(StorageNodeServiceGrpc.newBlockingStub(ch).withDeadline(deadline));
        } catch (StatusRuntimeException e) {
            boolean unavailable = e.getStatus().getCode() == Status.Code.UNAVAILABLE;
            if (!retryUnavailable || !unavailable || deadline.isExpired()) {
                failed(nc);
                if (unavailable) {
                    // No retry here, but the next call gets a fresh channel.
                    replace(node, nc, ch, ConnectivityState.TRANSIENT_FAILURE, 0, false);
                }
                throw e;
            }
            // The cached channel may hold a stale address or a dead connection.
            // Replace it, so the address is resolved again, and retry once.
            log.atInfo()
                    .addKeyValue("node_id", node.nodeId())
                    .addKeyValue("addr", node.internalAddr())
                    .log("{} UNAVAILABLE; resetting channel and retrying once: {}", rpc, e.getStatus());
            ManagedChannel fresh = replace(node, nc, ch, ConnectivityState.TRANSIENT_FAILURE, 0, false);
            try {
                return call.apply(StorageNodeServiceGrpc.newBlockingStub(fresh).withDeadline(deadline));
            } catch (StatusRuntimeException retryFailure) {
                failed(nc);
                throw retryFailure;
            }
        }
    }

    /**
     * The channel to call node on: the cached one if it is READY, or becomes
     * READY within half the remaining deadline; otherwise a replacement
     * (rate-limited). A channel in TRANSIENT_FAILURE is returned as it is: the
     * call fails fast on it with UNAVAILABLE, which replaces it without limit.
     */
    private ManagedChannel ready(Node node, NodeChannel nc, Deadline deadline) {
        ManagedChannel ch = nc.channel;
        long start = System.nanoTime();
        long budget = deadline.timeRemaining(TimeUnit.NANOSECONDS) / 2;
        ConnectivityState state = ch.getState(true); // true: an IDLE channel starts connecting
        while (state != ConnectivityState.READY && state != ConnectivityState.TRANSIENT_FAILURE
                && state != ConnectivityState.SHUTDOWN) {
            long left = budget - (System.nanoTime() - start);
            if (left <= 0) {
                break;
            }
            CountDownLatch changed = new CountDownLatch(1);
            ch.notifyWhenStateChanged(state, changed::countDown);
            try {
                changed.await(left, TimeUnit.NANOSECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            state = ch.getState(false);
        }
        if (state == ConnectivityState.READY || state == ConnectivityState.TRANSIENT_FAILURE) {
            return ch;
        }
        return replace(node, nc, ch, state, System.nanoTime() - start, true);
    }

    /**
     * Replaces nc's channel if it is still stale, and returns the channel to
     * use. Single-flight: callers that found the same stale channel get the
     * one replacement. rateLimited (a channel not ready in time): at most one
     * such replacement per replace interval per node; UNAVAILABLE replacements
     * are never limited.
     */
    private ManagedChannel replace(Node node, NodeChannel nc, ManagedChannel stale, ConnectivityState state,
            long notReadyNanos, boolean rateLimited) {
        ManagedChannel fresh;
        long replacements;
        synchronized (nc) {
            if (nc.channel != stale) {
                return nc.channel; // another call already replaced it
            }
            long now = System.nanoTime();
            if (rateLimited && nc.lastNotReadyReplacementNanos != Long.MIN_VALUE
                    && now - nc.lastNotReadyReplacementNanos < replaceIntervalNanos) {
                return stale; // replaced recently for the same reason: a node that never connects isn't churned
            }
            fresh = newChannel(nc.addr);
            nc.channel = fresh;
            if (rateLimited) {
                nc.lastNotReadyReplacementNanos = now;
                nc.notReadyReplacements++;
            }
            replacements = ++nc.replacements;
        }
        log.atWarn()
                .addKeyValue("node_id", node.nodeId())
                .addKeyValue("addr", nc.addr)
                .addKeyValue("channel_state", state)
                .addKeyValue("not_ready_ms", TimeUnit.NANOSECONDS.toMillis(notReadyNanos))
                .addKeyValue("replacements", replacements)
                .addKeyValue("reason", rateLimited ? "not ready in time" : "UNAVAILABLE")
                .log("node channel replaced: it was {} for {} ms", state, TimeUnit.NANOSECONDS.toMillis(notReadyNanos));
        retire(stale);
        return fresh;
    }

    /** Shuts a replaced channel down: in-flight calls get a grace (one node RPC deadline), then it is forced. */
    private void retire(ManagedChannel old) {
        old.shutdown();
        reaper.schedule(() -> {
            old.shutdownNow();
            try {
                old.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            if (old.isTerminated()) {
                live.remove(old);
            }
        }, timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    private ManagedChannel newChannel(String addr) {
        ManagedChannel ch = ManagedChannelBuilder.forTarget(addr).usePlaintext()
                .keepAliveTime(KEEPALIVE_TIME.toNanos(), TimeUnit.NANOSECONDS)
                .keepAliveTimeout(KEEPALIVE_TIMEOUT.toNanos(), TimeUnit.NANOSECONDS)
                .keepAliveWithoutCalls(true)
                .build();
        live.add(ch);
        created.incrementAndGet();
        return ch;
    }

    private void failed(NodeChannel nc) {
        long now = System.nanoTime();
        synchronized (nc) {
            nc.failures.addLast(now);
            prune(nc, now);
        }
    }

    private static void prune(NodeChannel nc, long now) {
        while (!nc.failures.isEmpty() && now - nc.failures.peekFirst() > FAILURE_WINDOW.toNanos()) {
            nc.failures.removeFirst();
        }
    }

    /** Channel replacements since this coordinator started, and failed calls in the last minute, for node. */
    public ChannelStats stats(Node node) {
        NodeChannel nc = channels.get(node.internalAddr());
        if (nc == null) {
            return new ChannelStats(0, 0);
        }
        synchronized (nc) {
            prune(nc, System.nanoTime());
            return new ChannelStats(nc.replacements, nc.failures.size());
        }
    }

    /** Channels created and not yet terminated, replaced ones included. For tests. */
    int liveChannels() {
        return live.size();
    }

    /** Channels ever created. For tests. */
    long createdChannels() {
        return created.get();
    }

    /** Replacements of a channel that wasn't ready in time (the rate-limited kind). For tests. */
    long notReadyReplacements(Node node) {
        NodeChannel nc = channels.get(node.internalAddr());
        if (nc == null) {
            return 0;
        }
        synchronized (nc) {
            return nc.notReadyReplacements;
        }
    }

    @Override
    public void destroy() {
        reaper.shutdownNow();
        live.forEach(ManagedChannel::shutdownNow);
    }
}
