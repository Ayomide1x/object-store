package io.github.ayomide1x.objectstore.coordinator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.google.protobuf.ByteString;
import io.github.ayomide1x.objectstore.v1.ChunkStat;
import io.github.ayomide1x.objectstore.v1.PushChunkRequest;
import io.github.ayomide1x.objectstore.v1.PushChunkResponse;
import io.github.ayomide1x.objectstore.v1.StatChunksRequest;
import io.github.ayomide1x.objectstore.v1.StatChunksResponse;
import io.github.ayomide1x.objectstore.v1.StorageNodeServiceGrpc;
import io.github.ayomide1x.objectstore.v1.VerifyChunkRequest;
import io.github.ayomide1x.objectstore.v1.VerifyChunkResponse;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

/**
 * Four in-process storage nodes answering StatChunks and VerifyChunk. Each
 * test decides what each node holds and how it behaves.
 */
final class FakeNodes {

    /** How a fake node answers its next calls. FAIL_ONCE fails the next call with UNAVAILABLE, then answers. */
    enum Mode { NORMAL, FAIL, FAIL_ONCE, HANG }

    /** How a fake node answers PushChunk as the source of a copy. */
    enum PushMode {
        COPY,         // copy its holding to the target, answer COPIED
        HAS_CHUNK,    // answer TARGET_HAS_CHUNK (the target already has a file)
        TARGET_FAIL,  // answer TARGET_FAILED
        DATA_LOSS,    // fail with DATA_LOSS: its own copy is bad
        LIE           // answer COPIED without copying anything
    }

    /** One PushChunk call, as observed. */
    record Push(String chunkId, String source, String target) {
    }

    /** What a node holds for one chunk. */
    record Held(ChunkStat.State state, long size, byte[] sha256) {
    }

    static final class FakeNode extends StorageNodeServiceGrpc.StorageNodeServiceImplBase {
        final String nodeId;
        final Map<String, Held> held = new ConcurrentHashMap<>();
        volatile Mode mode = Mode.NORMAL;
        volatile CountDownLatch release = new CountDownLatch(1);
        /** The node ID put in responses; normally this node's own. */
        volatile String answerAs;
        final AtomicInteger calls = new AtomicInteger();
        /**
         * What VerifyChunk reports per chunk ID. Without an entry: HEALTHY if
         * the node holds the chunk, otherwise MISSING.
         */
        final Map<String, VerifyChunkResponse.State> verifyAs = new ConcurrentHashMap<>();
        final AtomicInteger verifyCalls = new AtomicInteger();
        volatile PushMode pushMode = PushMode.COPY;
        FakeNodes owner;
        Server server;

        FakeNode(String nodeId) {
            this.nodeId = nodeId;
            this.answerAs = nodeId;
        }

        @Override
        public void statChunks(StatChunksRequest request, StreamObserver<StatChunksResponse> response) {
            calls.incrementAndGet();
            if (failedByMode(response)) {
                return;
            }
            StatChunksResponse.Builder b = StatChunksResponse.newBuilder().setNodeId(answerAs);
            for (String id : request.getChunkIdsList()) {
                Held h = held.get(id);
                ChunkStat.Builder s = ChunkStat.newBuilder().setChunkId(id);
                if (h == null) {
                    s.setState(ChunkStat.State.STATE_MISSING);
                } else {
                    s.setState(h.state());
                    if (h.state() == ChunkStat.State.STATE_FOUND) {
                        s.setSizeBytes(h.size()).setSha256(ByteString.copyFrom(h.sha256()));
                    }
                }
                b.addStats(s);
            }
            response.onNext(b.build());
            response.onCompleted();
        }

        @Override
        public void verifyChunk(VerifyChunkRequest request, StreamObserver<VerifyChunkResponse> response) {
            verifyCalls.incrementAndGet();
            if (failedByMode(response)) {
                return;
            }
            String id = request.getChunkId();
            VerifyChunkResponse.State state = verifyAs.getOrDefault(id,
                    held.containsKey(id) ? VerifyChunkResponse.State.STATE_HEALTHY : VerifyChunkResponse.State.STATE_MISSING);
            response.onNext(VerifyChunkResponse.newBuilder().setNodeId(answerAs).setChunkId(id).setState(state)
                    .setDetail(state == VerifyChunkResponse.State.STATE_CORRUPT ? "fake: data hashes wrong" : "").build());
            response.onCompleted();
        }

        @Override
        public void pushChunk(PushChunkRequest request, StreamObserver<PushChunkResponse> response) {
            owner.pushes.add(new Push(request.getChunkId(), nodeId, request.getTargetNodeId()));
            owner.enter(nodeId, request.getTargetNodeId());
            try {
                if (owner.pushDelayMs > 0) {
                    Thread.sleep(owner.pushDelayMs);
                }
                PushChunkResponse.Builder b = PushChunkResponse.newBuilder().setNodeId(nodeId).setChunkId(request.getChunkId());
                switch (pushMode) {
                    case DATA_LOSS -> {
                        response.onError(Status.DATA_LOSS.withDescription("fake: source copy corrupt").asRuntimeException());
                        return;
                    }
                    case HAS_CHUNK -> b.setOutcome(PushChunkResponse.Outcome.OUTCOME_TARGET_HAS_CHUNK);
                    case TARGET_FAIL -> b.setOutcome(PushChunkResponse.Outcome.OUTCOME_TARGET_FAILED).setDetail("fake: target down");
                    case LIE -> b.setOutcome(PushChunkResponse.Outcome.OUTCOME_COPIED);
                    case COPY -> {
                        Held h = held.get(request.getChunkId());
                        owner.get(request.getTargetNodeId()).held.put(request.getChunkId(), h);
                        b.setOutcome(PushChunkResponse.Outcome.OUTCOME_COPIED);
                    }
                }
                response.onNext(b.build());
                response.onCompleted();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                response.onError(Status.CANCELLED.asRuntimeException());
            } finally {
                owner.leave(nodeId, request.getTargetNodeId());
            }
        }

        /** Applies the current mode; true if the call has been answered with an error. */
        private boolean failedByMode(StreamObserver<?> response) {
            switch (mode) {
                case FAIL_ONCE -> {
                    mode = Mode.NORMAL;
                    response.onError(Status.UNAVAILABLE.withDescription("fake node failing once").asRuntimeException());
                    return true;
                }
                case FAIL -> {
                    response.onError(Status.UNAVAILABLE.withDescription("fake node failing").asRuntimeException());
                    return true;
                }
                case HANG -> {
                    try {
                        release.await(); // until the test ends: never within the coordinator's deadline
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    response.onError(Status.UNAVAILABLE.asRuntimeException());
                    return true;
                }
                case NORMAL -> {
                    return false;
                }
            }
            return false;
        }
    }

    private final List<FakeNode> nodes = new ArrayList<>();

    /** Every PushChunk call, in order of arrival. */
    final List<Push> pushes = new java.util.concurrent.CopyOnWriteArrayList<>();
    /** How long each push takes, to make concurrency observable. */
    volatile long pushDelayMs;
    private int inFlight;
    private int maxInFlight;
    private final Map<String, Integer> perNode = new java.util.HashMap<>();
    private final Map<String, Integer> maxPerNode = new java.util.HashMap<>();

    synchronized void enter(String source, String target) {
        maxInFlight = Math.max(maxInFlight, ++inFlight);
        for (String n : List.of(source, target)) {
            int v = perNode.merge(n, 1, Integer::sum);
            maxPerNode.merge(n, v, Math::max);
        }
    }

    synchronized void leave(String source, String target) {
        inFlight--;
        perNode.merge(source, -1, Integer::sum);
        perNode.merge(target, -1, Integer::sum);
    }

    synchronized int maxInFlight() {
        return maxInFlight;
    }

    synchronized int maxOnAnyNode() {
        return maxPerNode.values().stream().mapToInt(Integer::intValue).max().orElse(0);
    }

    FakeNodes(int count) {
        for (int i = 1; i <= count; i++) {
            FakeNode node = new FakeNode("node" + i);
            node.owner = this;
            try {
                node.server = ServerBuilder.forPort(0).addService(node).build().start();
            } catch (IOException e) {
                throw new IllegalStateException("start fake " + node.nodeId, e);
            }
            nodes.add(node);
        }
    }

    /** The COORDINATOR_NODES value registering these fakes. */
    String nodesProperty() {
        return nodes.stream()
                .map(n -> "%s|127.0.0.1:%d|127.0.0.1:%d".formatted(n.nodeId, n.server.getPort(), n.server.getPort()))
                .collect(Collectors.joining(","));
    }

    FakeNode get(String nodeId) {
        return nodes.stream().filter(n -> n.nodeId.equals(nodeId)).findFirst().orElseThrow();
    }

    /** Forgets every holding and returns every node to normal, releasing any hang. */
    void reset() {
        pushes.clear();
        pushDelayMs = 0;
        synchronized (this) {
            inFlight = 0;
            maxInFlight = 0;
            perNode.clear();
            maxPerNode.clear();
        }
        for (FakeNode n : nodes) {
            n.held.clear();
            n.mode = Mode.NORMAL;
            n.answerAs = n.nodeId;
            n.calls.set(0);
            n.verifyAs.clear();
            n.verifyCalls.set(0);
            n.pushMode = PushMode.COPY;
            n.release.countDown();
            n.release = new CountDownLatch(1);
        }
    }
}
