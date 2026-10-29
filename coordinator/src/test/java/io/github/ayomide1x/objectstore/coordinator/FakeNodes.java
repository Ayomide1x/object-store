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
import io.github.ayomide1x.objectstore.v1.StatChunksRequest;
import io.github.ayomide1x.objectstore.v1.StatChunksResponse;
import io.github.ayomide1x.objectstore.v1.StorageNodeServiceGrpc;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

/**
 * Four in-process storage nodes answering StatChunks. Each test decides what
 * each node holds and how it behaves.
 */
final class FakeNodes {

    /** How a fake node answers StatChunks. FAIL_ONCE fails the next call with UNAVAILABLE, then answers. */
    enum Mode { NORMAL, FAIL, FAIL_ONCE, HANG }

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
        Server server;

        FakeNode(String nodeId) {
            this.nodeId = nodeId;
            this.answerAs = nodeId;
        }

        @Override
        public void statChunks(StatChunksRequest request, StreamObserver<StatChunksResponse> response) {
            calls.incrementAndGet();
            switch (mode) {
                case FAIL_ONCE -> {
                    mode = Mode.NORMAL;
                    response.onError(Status.UNAVAILABLE.withDescription("fake node failing once").asRuntimeException());
                    return;
                }
                case FAIL -> {
                    response.onError(Status.UNAVAILABLE.withDescription("fake node failing").asRuntimeException());
                    return;
                }
                case HANG -> {
                    try {
                        release.await(); // until the test ends: never within the coordinator's deadline
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    response.onError(Status.UNAVAILABLE.asRuntimeException());
                    return;
                }
                case NORMAL -> {
                }
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
    }

    private final List<FakeNode> nodes = new ArrayList<>();

    FakeNodes(int count) {
        for (int i = 1; i <= count; i++) {
            FakeNode node = new FakeNode("node" + i);
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
        for (FakeNode n : nodes) {
            n.held.clear();
            n.mode = Mode.NORMAL;
            n.answerAs = n.nodeId;
            n.calls.set(0);
            n.release.countDown();
            n.release = new CountDownLatch(1);
        }
    }
}
