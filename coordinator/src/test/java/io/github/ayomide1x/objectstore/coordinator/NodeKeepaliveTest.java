package io.github.ayomide1x.objectstore.coordinator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import io.github.ayomide1x.objectstore.v1.StatChunksRequest;
import io.github.ayomide1x.objectstore.v1.StatChunksResponse;
import io.github.ayomide1x.objectstore.v1.StorageNodeServiceGrpc;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A half-open connection to a node: a proxy in front of a real gRPC node
 * server stops forwarding on the open connection and closes nothing. Without
 * keepalive the channel stays READY and every call times out; NodeClient's
 * keepalive closes the connection within KEEPALIVE_TIME + KEEPALIVE_TIMEOUT,
 * and the next call reconnects.
 */
class NodeKeepaliveTest {

    /** The longest a half-open connection may last: one ping interval plus its timeout, plus 1 s. */
    static final Duration DETECTION = NodeClient.KEEPALIVE_TIME.plus(NodeClient.KEEPALIVE_TIMEOUT)
            .plusSeconds(1);

    final List<AutoCloseable> cleanup = new ArrayList<>();
    HalfOpenProxy proxy;

    @BeforeEach
    void start() throws IOException {
        InetAddress loopback = InetAddress.getLoopbackAddress();
        Server s = NettyServerBuilder.forAddress(new InetSocketAddress(loopback, 0))
                .permitKeepAliveTime(5, TimeUnit.SECONDS).permitKeepAliveWithoutCalls(true) // as a real node
                .addService(new StorageNodeServiceGrpc.StorageNodeServiceImplBase() {
                    @Override
                    public void statChunks(StatChunksRequest req, StreamObserver<StatChunksResponse> out) {
                        out.onNext(StatChunksResponse.newBuilder().setNodeId("node3").build());
                        out.onCompleted();
                    }
                })
                .build().start();
        cleanup.add(() -> s.shutdownNow().awaitTermination(5, TimeUnit.SECONDS));
        proxy = new HalfOpenProxy(new InetSocketAddress(loopback, s.getPort()));
        cleanup.add(proxy);
    }

    @AfterEach
    void close() throws Exception {
        for (AutoCloseable c : cleanup.reversed()) {
            c.close();
        }
    }

    String proxyAddr() {
        return InetAddress.getLoopbackAddress().getHostAddress() + ":" + proxy.port();
    }

    /**
     * The control: the proxy really makes a half-open connection. A channel
     * without keepalive stays READY, on its one connection, and every call
     * times out for longer than keepalive needs to notice.
     */
    @Test
    @Timeout(60)
    void withoutKeepaliveAHalfOpenConnectionStaysReadyAndCallsTimeOut() throws Exception {
        ManagedChannel ch = ManagedChannelBuilder.forTarget(proxyAddr()).usePlaintext().build();
        cleanup.add(() -> ch.shutdownNow().awaitTermination(5, TimeUnit.SECONDS));
        StatChunksRequest req = StatChunksRequest.getDefaultInstance();
        StorageNodeServiceGrpc.newBlockingStub(ch).withDeadlineAfter(2, TimeUnit.SECONDS).statChunks(req);

        proxy.freeze();
        long end = System.nanoTime() + DETECTION.toNanos();
        int calls = 0;
        while (System.nanoTime() < end) {
            assertThatThrownBy(() -> StorageNodeServiceGrpc.newBlockingStub(ch)
                    .withDeadlineAfter(1, TimeUnit.SECONDS).statChunks(req))
                    .isInstanceOfSatisfying(StatusRuntimeException.class,
                            e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.DEADLINE_EXCEEDED));
            calls++;
        }
        assertThat(ch.getState(false)).isEqualTo(ConnectivityState.READY);
        assertThat(proxy.accepted()).isEqualTo(1);
        assertThat(calls).isGreaterThan(5);
    }

    /**
     * With NodeClient's keepalive the half-open connection is closed within
     * DETECTION, and a call after that reaches the node on a new connection.
     * The reconnect is the ordinary one: no not-ready replacement, so the
     * replacement rate limit never comes into it.
     */
    @Test
    @Timeout(60)
    void aHalfOpenNodeConnectionIsClosedAndTheNextCallReconnects() throws Exception {
        CoordinatorProperties p = new CoordinatorProperties("node3|" + proxyAddr() + "|" + proxyAddr(),
                Duration.ofSeconds(2), Duration.ofSeconds(6), Duration.ofSeconds(15), 2, Duration.ZERO,
                Duration.ofSeconds(30), Duration.ZERO, 8, 2, Duration.ofSeconds(30), Duration.ofHours(24),
                Duration.ZERO, Duration.ofMinutes(15), Duration.ofHours(24), 256, 4, 1, Duration.ofHours(24),
                Duration.ofSeconds(1), Duration.ofHours(24), false, Duration.ofSeconds(5));
        NodeClient c = new NodeClient(p);
        cleanup.add(c::destroy);
        Node n3 = new Node("node3", proxyAddr(), proxyAddr());
        assertThat(answered(c, n3)).isTrue();

        proxy.freeze();
        long frozenAt = System.nanoTime();
        assertThat(answered(c, n3)).as("a call on the half-open connection").isFalse();

        boolean back = false;
        while (!back && System.nanoTime() - frozenAt < DETECTION.plusSeconds(2).toNanos()) { // + one call
            back = answered(c, n3);
        }
        assertThat(back).as("a call reached the node within %s of the freeze", DETECTION.plusSeconds(2)).isTrue();
        assertThat(proxy.accepted()).as("connections").isEqualTo(2);
        assertThat(c.notReadyReplacements(n3)).isZero();
    }

    static boolean answered(NodeClient c, Node n) {
        Map<String, ?> r = c.statChunks(UUID.randomUUID(), Map.of(n, List.of(UUID.randomUUID())));
        return r.containsKey("node3");
    }
}
