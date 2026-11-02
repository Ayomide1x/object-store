package io.github.ayomide1x.objectstore.coordinator;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import io.github.ayomide1x.objectstore.v1.StatChunksRequest;
import io.github.ayomide1x.objectstore.v1.StatChunksResponse;
import io.github.ayomide1x.objectstore.v1.StorageNodeServiceGrpc;
import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * NodeClient's channels to nodes whose address changes, or that stop
 * answering, against real gRPC servers and a DNS the test controls
 * (TestDns). The old address of a moved node is taken by a "silent" peer that
 * accepts TCP but never speaks HTTP/2, so a channel connecting there stays
 * CONNECTING forever without failing: the stuck state the coordinator met on
 * the live stack (calls end in DEADLINE_EXCEEDED, never UNAVAILABLE, so the
 * old UNAVAILABLE-only reset never ran).
 */
class NodeAddressChangeTest {

    /**
     * Each test its own name: the JVM caches failed lookups for 10 s
     * (networkaddress.cache.negative.ttl in java.security, which a system
     * property doesn't override), so a name one test leaves unresolvable
     * would fail the next test's lookups.
     */
    final String HOST = "node3-" + UUID.randomUUID() + ".test";

    final List<AutoCloseable> cleanup = new ArrayList<>();

    @AfterEach
    void close() throws Exception {
        TestDns.reset();
        for (AutoCloseable c : cleanup.reversed()) {
            c.close();
        }
    }

    /** A fake node answering StatChunks as nodeId, on addr:port. */
    Server node(String nodeId, InetAddress addr, int port) throws IOException {
        Server s = NettyServerBuilder.forAddress(new InetSocketAddress(addr, port))
                .addService(new StorageNodeServiceGrpc.StorageNodeServiceImplBase() {
                    @Override
                    public void statChunks(StatChunksRequest req, StreamObserver<StatChunksResponse> out) {
                        out.onNext(StatChunksResponse.newBuilder().setNodeId(nodeId).build());
                        out.onCompleted();
                    }
                })
                .build().start();
        cleanup.add(() -> s.shutdownNow().awaitTermination(5, TimeUnit.SECONDS));
        return s;
    }

    /**
     * Accepts connections on addr:port and never answers: a transport that
     * never becomes ready. Closing it closes the accepted connections too, so
     * the address then refuses.
     */
    AutoCloseable silent(InetAddress addr, int port) throws IOException {
        ServerSocket ss = new ServerSocket();
        ss.setReuseAddress(true);
        ss.bind(new InetSocketAddress(addr, port));
        List<Socket> held = java.util.Collections.synchronizedList(new ArrayList<>());
        Thread t = Thread.ofVirtual().start(() -> {
            try {
                while (true) {
                    held.add(ss.accept());
                }
            } catch (IOException e) {
                // closed
            }
        });
        AutoCloseable closer = () -> {
            ss.close();
            synchronized (held) {
                for (Socket s : held) {
                    s.close();
                }
            }
            t.join();
        };
        cleanup.add(closer);
        return closer;
    }

    static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    /** Long by default, as in production; tests of the rate limit set their own. */
    Duration replaceInterval = Duration.ofSeconds(5);

    NodeClient client() {
        CoordinatorProperties p = new CoordinatorProperties("node3|" + HOST + ":1|" + HOST + ":1",
                Duration.ofSeconds(2), Duration.ofSeconds(6), Duration.ofSeconds(15), 2, Duration.ZERO,
                Duration.ofSeconds(30), Duration.ZERO, 8, 2, Duration.ofSeconds(30), Duration.ofHours(24),
                Duration.ZERO, Duration.ofMinutes(15), Duration.ofHours(24), 256, 4, 1, Duration.ofHours(24),
                Duration.ofSeconds(1), Duration.ofHours(24), false, replaceInterval);
        NodeClient c = new NodeClient(p);
        cleanup.add(c::destroy);
        return c;
    }

    boolean statAnswered(NodeClient c, Node n) {
        Map<String, ?> r = c.statChunks(UUID.randomUUID(), Map.of(n, List.of(UUID.randomUUID())));
        return r.containsKey("node3");
    }

    /**
     * grpc-java issue 12395's sequence: the new address arrives while the
     * channel reconnects to the old one. On the old code this stuck about 4
     * runs in 10, depending on gRPC's internal thread timing; it is kept as a
     * second scenario, not as the regression gate (the next test is that).
     */
    @Test
    void callsReachTheNodeAfterItsAddressChangesMidReconnect() throws Exception {
        InetAddress oldAddr = InetAddress.getByName("127.0.0.1");
        InetAddress newAddr = InetAddress.getByName("::1");
        int port = freePort();
        Node n3 = new Node("node3", HOST + ":" + port, HOST + ":" + port);
        TestDns.names.put(HOST, oldAddr);
        Server first = node("node3", oldAddr, port);
        NodeClient c = client();
        assertThat(statAnswered(c, n3)).as("reachable at the old address").isTrue();

        // The node goes away; its old address now accepts but never answers.
        first.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        silent(oldAddr, port);
        // Lookups wait: the channel reconnects to the address it has.
        TestDns.hold = new CountDownLatch(1);
        TestDns.names.put(HOST, newAddr);
        node("node3", newAddr, port);
        statAnswered(c, n3);       // reconnecting to the silent address
        TestDns.hold.countDown();  // the new address arrives mid-reconnect

        List<Boolean> after = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            after.add(statAnswered(c, n3));
        }
        assertThat(after).as("every call after the new address is known reaches the node").containsOnly(true);
    }

    /**
     * The regression gate, deterministic: the node moves and something silent
     * takes its old address while DNS still points there, so the channel
     * connects to it and stays CONNECTING; then DNS points to the node. The
     * old code never reached the node again (every call DEADLINE_EXCEEDED).
     * Mutation: the readiness check before each call is removed.
     */
    @Test
    void aChannelThatNeverBecomesReadyIsReplaced() throws Exception {
        InetAddress oldAddr = InetAddress.getByName("127.0.0.1");
        InetAddress newAddr = InetAddress.getByName("::1");
        int port = freePort();
        Node n3 = new Node("node3", HOST + ":" + port, HOST + ":" + port);
        TestDns.names.put(HOST, oldAddr);
        Server first = node("node3", oldAddr, port);
        NodeClient c = client();
        assertThat(statAnswered(c, n3)).as("reachable at the old address").isTrue();

        // The node leaves; something silent takes its old address, and the
        // next call reconnects there (DNS still says the old address).
        first.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        silent(oldAddr, port);
        assertThat(statAnswered(c, n3)).as("the silent peer answers nothing").isFalse();

        // Now DNS says where the node is. Within one replace interval plus
        // one call, calls reach it, and keep reaching it. (The call above may
        // already have used this node's replacement for the interval.)
        node("node3", newAddr, port);
        TestDns.names.put(HOST, newAddr);
        long deadline = System.nanoTime() + (replaceInterval.plusSeconds(4)).toNanos();
        boolean reached = false;
        while (!reached && System.nanoTime() < deadline) {
            reached = statAnswered(c, n3);
        }
        assertThat(reached).as("a call reaches the node at its new address within the replace interval").isTrue();
        for (int i = 0; i < 3; i++) {
            assertThat(statAnswered(c, n3)).as("and every call after").isTrue();
        }
        assertThat(c.stats(n3).replacements()).isBetween(1L, 2L);
    }

    /**
     * Mutation: replacement is not single-flight (every caller replaces). The
     * rate limit is off here, so single-flight alone must hold it to one.
     */
    @Test
    void concurrentCallsOnAStuckChannelReplaceItOnce() throws Exception {
        replaceInterval = Duration.ZERO;
        InetAddress addr = InetAddress.getByName("127.0.0.1");
        int port = freePort();
        Node n3 = new Node("node3", HOST + ":" + port, HOST + ":" + port);
        TestDns.names.put(HOST, addr);
        silent(addr, port); // never ready
        NodeClient c = client();

        List<Thread> callers = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            callers.add(Thread.ofVirtual().start(() -> statAnswered(c, n3)));
        }
        for (Thread t : callers) {
            t.join();
        }

        assertThat(c.stats(n3).replacements()).as("8 calls found the same stuck channel").isEqualTo(1);
    }

    /**
     * A node that is really down doesn't get a new channel on every call.
     * Mutation: the rate limit is removed.
     */
    @Test
    void replacementIsRateLimitedPerNode() throws Exception {
        InetAddress addr = InetAddress.getByName("127.0.0.1");
        int port = freePort();
        Node n3 = new Node("node3", HOST + ":" + port, HOST + ":" + port);
        TestDns.names.put(HOST, addr);
        silent(addr, port);
        NodeClient c = client();

        for (int i = 0; i < 3; i++) {
            assertThat(statAnswered(c, n3)).isFalse();
        }

        // 3 calls of 2 s each span about 6 s: one replacement, or two if
        // the third call comes after the 5 s interval, never three.
        assertThat(c.stats(n3).replacements()).as("at most one replacement per 5 s").isBetween(1L, 2L);
        assertThat(c.stats(n3).recentFailures()).as("every failed call counted").isEqualTo(3);
    }

    /** Replaced channels are shut down: replacements don't leak channels. */
    @Test
    void replacedChannelsAreShutDown() throws Exception {
        replaceInterval = Duration.ZERO;
        InetAddress addr = InetAddress.getByName("127.0.0.1");
        int port = freePort();
        Node n3 = new Node("node3", HOST + ":" + port, HOST + ":" + port);
        TestDns.names.put(HOST, addr);
        silent(addr, port);
        NodeClient c = client();

        for (int i = 0; i < 4; i++) {
            statAnswered(c, n3);
        }
        assertThat(c.stats(n3).replacements()).isGreaterThanOrEqualTo(4);

        // Each replaced channel is forced down one node deadline (2 s) after
        // its replacement; then only the current channel is left.
        long until = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (c.liveChannels() > 1 && System.nanoTime() < until) {
            Thread.sleep(100);
        }
        assertThat(c.liveChannels()).as("only the current channel is still open").isEqualTo(1);
    }

    /**
     * Stage 4 run 1's sequence, which the first version of the fix broke: a
     * node's channel is replaced (starting its rate-limit window), the node
     * goes down and is called while down, then returns on a new address
     * within the window. The first call after it returns must reach it: an
     * UNAVAILABLE replace-and-retry is never rate-limited.
     * Mutation: the rate limit applies to the UNAVAILABLE path again.
     */
    @Test
    void theFirstCallAfterANodeReturnsOnANewAddressSucceedsWithinTheWindow() throws Exception {
        replaceInterval = Duration.ofMinutes(5); // the whole test is inside one window
        InetAddress oldAddr = InetAddress.getByName("127.0.0.1");
        InetAddress newAddr = InetAddress.getByName("::1");
        int port = freePort();
        Node n3 = new Node("node3", HOST + ":" + port, HOST + ":" + port);
        TestDns.names.put(HOST, oldAddr);
        NodeClient c = client();

        // A channel that never becomes ready is replaced: the window starts.
        AutoCloseable silentPeer = silent(oldAddr, port);
        assertThat(statAnswered(c, n3)).isFalse();
        assertThat(c.stats(n3).replacements()).as("the window has started").isEqualTo(1);

        // The node is down: its old address refuses connections; calls fail.
        silentPeer.close();
        assertThat(statAnswered(c, n3)).isFalse();

        // It returns on a new address, well inside the window.
        node("node3", newAddr, port);
        TestDns.names.put(HOST, newAddr);
        assertThat(statAnswered(c, n3)).as("the first call after the node returns reaches it").isTrue();
    }

    /** A node that refuses connections: at most one new channel per call, and none left open. */
    @Test
    void aRefusingNodeGetsAtMostOneChannelPerCallAndNoneLeak() throws Exception {
        InetAddress addr = InetAddress.getByName("127.0.0.1");
        int port = freePort(); // nothing listens: every connection is refused
        Node n3 = new Node("node3", HOST + ":" + port, HOST + ":" + port);
        TestDns.names.put(HOST, addr);
        NodeClient c = client();

        int calls = 5;
        for (int i = 0; i < calls; i++) {
            assertThat(statAnswered(c, n3)).isFalse();
        }

        assertThat(c.createdChannels() - 1).as("new channels, beyond the first").isLessThanOrEqualTo(calls);
        assertThat(c.stats(n3).recentFailures()).isEqualTo(calls);
        long until = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (c.liveChannels() > 1 && System.nanoTime() < until) {
            Thread.sleep(100);
        }
        assertThat(c.liveChannels()).as("every replaced channel was shut down").isEqualTo(1);
    }
}
