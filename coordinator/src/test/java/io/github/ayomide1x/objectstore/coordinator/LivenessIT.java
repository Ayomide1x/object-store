package io.github.ayomide1x.objectstore.coordinator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import io.github.ayomide1x.objectstore.coordinator.FakeNodes.Held;
import io.github.ayomide1x.objectstore.coordinator.FakeNodes.Mode;
import io.github.ayomide1x.objectstore.coordinator.LivenessTrackerTest.FakeTicker;
import io.github.ayomide1x.objectstore.coordinator.MetadataService.ChunkPlacement;
import io.github.ayomide1x.objectstore.coordinator.MetadataService.CompletedChunk;
import io.github.ayomide1x.objectstore.coordinator.MetadataService.UploadPlan;
import io.github.ayomide1x.objectstore.v1.ChunkStat;
import io.github.ayomide1x.objectstore.v1.GetObjectRequest;
import io.github.ayomide1x.objectstore.v1.GetObjectResponse;
import io.github.ayomide1x.objectstore.v1.NodeLiveness;
import io.github.ayomide1x.objectstore.v1.NodeLocation;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * What liveness changes: placement, CompleteUpload and GetObject. Time is a
 * fake ticker and the sweeper is off: tests move the clock and sweep by hand.
 */
@Testcontainers
@SpringBootTest(properties = {
        "spring.grpc.server.port=0",
        "coordinator.node-rpc-timeout=" + LivenessIT.NODE_TIMEOUT_MS + "ms",
        "coordinator.liveness-sweep=0",
        "coordinator.repair-interval=0",
})
class LivenessIT {

    static final long NODE_TIMEOUT_MS = 500;
    static final List<String> ALL = List.of("node1", "node2", "node3", "node4");

    @Container
    @ServiceConnection
    static PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:18");

    static final FakeNodes nodes = new FakeNodes(4);
    static final FakeTicker clock = new FakeTicker();

    @DynamicPropertySource
    static void registerNodes(DynamicPropertyRegistry registry) {
        registry.add("coordinator.nodes", nodes::nodesProperty);
    }

    @TestConfiguration
    static class Clock {
        @Bean
        @Primary
        Ticker fakeTicker() {
            return clock;
        }
    }

    private final MetadataService metadata;
    private final LivenessService liveness;
    private final CoordinatorGrpcService grpc;
    private final JdbcClient jdbc;

    @Autowired
    LivenessIT(MetadataService metadata, LivenessService liveness, CoordinatorGrpcService grpc, JdbcClient jdbc) {
        this.metadata = metadata;
        this.liveness = liveness;
        this.grpc = grpc;
        this.jdbc = jdbc;
    }

    @BeforeEach
    void allAlive() {
        jdbc.sql("TRUNCATE keys, chunk_replicas, chunk_placements, chunks, objects").update();
        nodes.reset();
        // Two beats: enough to bring back a node an earlier test left DEAD.
        for (int i = 0; i < 2; i++) {
            ALL.forEach(n -> liveness.heartbeat(n, "test-instance"));
        }
        assertThat(ALL).allSatisfy(n -> assertThat(liveness.stateOf(n)).isEqualTo(LivenessTracker.State.ALIVE));
    }

    @AfterEach
    void releaseHangs() {
        nodes.reset();
    }

    /** Leaves every node except the given ones silent long enough to be DEAD. */
    private void onlyAlive(String... alive) {
        clock.advance(Duration.ofSeconds(16));
        for (String n : alive) {
            liveness.heartbeat(n, "test-instance");
        }
        liveness.sweep();
    }

    /** Mutation: placement includes non-ALIVE nodes. */
    @Test
    void placementUsesOnlyAliveNodes() {
        onlyAlive("node1", "node2", "node3");
        UploadPlan plan = metadata.initiateUpload("alive-only", 8 * MetadataService.CHUNK_SIZE);
        for (ChunkPlacement c : plan.chunks()) {
            assertThat(nodeIds(c)).as("chunk %d", c.index()).containsExactlyInAnyOrder("node1", "node2", "node3");
        }
    }

    @Test
    void withExactlyTwoAliveNodesEveryChunkGoesToBoth() {
        onlyAlive("node2", "node4");
        UploadPlan plan = metadata.initiateUpload("two-alive", 3 * MetadataService.CHUNK_SIZE);
        for (ChunkPlacement c : plan.chunks()) {
            assertThat(nodeIds(c)).containsExactlyInAnyOrder("node2", "node4");
        }
        complete(plan, "two-alive", 3 * MetadataService.CHUNK_SIZE);
        assertThat(jdbc.sql("SELECT status FROM objects WHERE object_id = :id").param("id", plan.objectId())
                .query(String.class).single()).isEqualTo("COMMITTED");
    }

    @Test
    void withFewerThanTwoAliveNodesInitiateIsUnavailable() {
        onlyAlive("node3");
        assertThatThrownBy(() -> metadata.initiateUpload("one-alive", 10))
                .isInstanceOfSatisfying(RequestException.class,
                        e -> assertThat(e.code()).isEqualTo(Status.Code.UNAVAILABLE))
                .hasMessageContaining("1 of 4 storage nodes alive, need 2");
        // A 0-byte object places nothing, so it needs no node.
        UploadPlan empty = metadata.initiateUpload("empty", 0);
        metadata.completeUpload(empty.objectId(), "empty", 0, List.of());
    }

    /** Mutation: CompleteUpload asks DEAD nodes. */
    @Test
    @Timeout(value = 20, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void completeUploadDoesNotAskADeadNode() {
        UploadPlan plan = metadata.initiateUpload("dead-skip", 4 * MetadataService.CHUNK_SIZE);
        holdOnPlacedNodes(plan);
        String dead = "node3";
        nodes.get(dead).mode = Mode.HANG; // asking it would cost the whole deadline
        onlyAlive("node1", "node2", "node4");
        assertThat(liveness.stateOf(dead)).isEqualTo(LivenessTracker.State.DEAD);

        long start = System.nanoTime();
        complete(plan, "dead-skip", 4 * MetadataService.CHUNK_SIZE);
        Duration elapsed = Duration.ofNanos(System.nanoTime() - start);

        assertThat(elapsed).as("no deadline spent on the DEAD node").isLessThan(Duration.ofMillis(NODE_TIMEOUT_MS));
        assertThat(nodes.get(dead).calls.get()).as("calls to the DEAD node").isZero();
        assertThat(jdbc.sql("""
                SELECT count(*) FROM chunk_replicas r JOIN chunks c USING (chunk_id)
                WHERE c.object_id = :id AND r.node_id = :n
                """).param("id", plan.objectId()).param("n", dead).query(Long.class).single()).isZero();
    }

    @Test
    void completeUploadStillAsksASuspectNodeAndCountsIt() {
        UploadPlan plan = metadata.initiateUpload("suspect-asked", 10);
        holdOnPlacedNodes(plan);
        String suspect = nodeIds(plan.chunks().getFirst()).getFirst();
        clock.advance(Duration.ofSeconds(7));
        ALL.stream().filter(n -> !n.equals(suspect)).forEach(n -> liveness.heartbeat(n, "test-instance"));
        liveness.sweep();
        assertThat(liveness.stateOf(suspect)).isEqualTo(LivenessTracker.State.SUSPECT);

        complete(plan, "suspect-asked", 10);
        assertThat(jdbc.sql("SELECT node_id FROM chunk_replicas WHERE chunk_id = :c ORDER BY node_id")
                .param("c", plan.chunks().getFirst().chunkId()).query(String.class).list())
                .contains(suspect).hasSize(3);
    }

    @Test
    void getObjectReportsEachReplicasLiveness() {
        UploadPlan plan = metadata.initiateUpload("liveness-listed", 10);
        complete(plan, "liveness-listed", 10);
        List<String> placed = nodeIds(plan.chunks().getFirst());
        String dead = placed.get(0);
        String suspect = placed.get(1);
        clock.advance(Duration.ofSeconds(7));
        ALL.stream().filter(n -> !n.equals(dead) && !n.equals(suspect)).forEach(n -> liveness.heartbeat(n, "test-instance"));
        liveness.sweep(); // dead and suspect are both SUSPECT now
        clock.advance(Duration.ofSeconds(9));
        ALL.stream().filter(n -> !n.equals(dead)).forEach(n -> liveness.heartbeat(n, "test-instance"));
        liveness.sweep(); // dead: 16 s silent → DEAD; suspect beat again → ALIVE
        clock.advance(Duration.ofSeconds(7));
        ALL.stream().filter(n -> !n.equals(dead) && !n.equals(suspect)).forEach(n -> liveness.heartbeat(n, "test-instance"));
        liveness.sweep(); // suspect: 7 s silent → SUSPECT

        List<NodeLocation> replicas = callGetObject("liveness-listed").getChunks(0).getReplicasList();
        assertThat(replicas).hasSize(3).allSatisfy(r -> {
            NodeLiveness want = r.getNodeId().equals(dead) ? NodeLiveness.NODE_LIVENESS_DEAD
                    : r.getNodeId().equals(suspect) ? NodeLiveness.NODE_LIVENESS_SUSPECT
                    : NodeLiveness.NODE_LIVENESS_ALIVE;
            assertThat(r.getLiveness()).as(r.getNodeId()).isEqualTo(want);
        });
    }

    @Test
    void transitionsAndHeartbeatsArePersisted() {
        onlyAlive("node1", "node2", "node4");
        assertThat(persistedState("node3")).isEqualTo("DEAD");
        assertThat(persistedState("node1")).isEqualTo("ALIVE");
        assertThat(jdbc.sql("SELECT state_changed_at IS NOT NULL AND last_heartbeat_at IS NOT NULL AND instance_id = 'test-instance' FROM nodes WHERE node_id = 'node1'")
                .query(Boolean.class).single()).isTrue();

        // Startup writes UNKNOWN for every node: after a restart nothing is known.
        liveness.afterPropertiesSet();
        assertThat(ALL).allSatisfy(n -> assertThat(persistedState(n)).isEqualTo("UNKNOWN"));
    }

    private GetObjectResponse callGetObject(String key) {
        List<GetObjectResponse> out = new ArrayList<>();
        List<Throwable> errors = new ArrayList<>();
        grpc.getObject(GetObjectRequest.newBuilder().setKey(key).build(), new StreamObserver<>() {
            public void onNext(GetObjectResponse v) { out.add(v); }
            public void onError(Throwable t) { errors.add(t); }
            public void onCompleted() { }
        });
        assertThat(errors).isEmpty();
        return out.getFirst();
    }

    private String persistedState(String node) {
        return jdbc.sql("SELECT state FROM nodes WHERE node_id = :n").param("n", node).query(String.class).single();
    }

    private void complete(UploadPlan plan, String key, long size) {
        holdOnPlacedNodes(plan);
        List<CompletedChunk> chunks = plan.chunks().stream()
                .map(c -> new CompletedChunk(c.chunkId(), c.index(), c.sizeBytes(), sha(c.chunkId()))).toList();
        metadata.completeUpload(plan.objectId(), key, size, chunks);
    }

    private static void holdOnPlacedNodes(UploadPlan plan) {
        for (ChunkPlacement c : plan.chunks()) {
            for (String n : nodeIds(c)) {
                nodes.get(n).held.put(c.chunkId().toString(), new Held(ChunkStat.State.STATE_FOUND, c.sizeBytes(), sha(c.chunkId())));
            }
        }
    }

    private static byte[] sha(UUID id) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(id.toString().getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private static List<String> nodeIds(ChunkPlacement c) {
        return c.nodes().stream().map(Node::nodeId).toList();
    }
}
