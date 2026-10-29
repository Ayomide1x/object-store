package io.github.ayomide1x.objectstore.coordinator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import io.github.ayomide1x.objectstore.coordinator.FakeNodes.Held;
import io.github.ayomide1x.objectstore.coordinator.FakeNodes.Mode;
import io.github.ayomide1x.objectstore.coordinator.MetadataService.ChunkPlacement;
import io.github.ayomide1x.objectstore.coordinator.MetadataService.CompletedChunk;
import io.github.ayomide1x.objectstore.coordinator.MetadataService.UploadPlan;
import io.github.ayomide1x.objectstore.v1.ChunkStat;
import io.github.ayomide1x.objectstore.v1.ReportBadReplicaResponse.Verdict;
import io.github.ayomide1x.objectstore.v1.VerifyChunkResponse;
import io.grpc.Status;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Bad-replica reports: the coordinator marks a replica BAD only when its own
 * node confirms it, and GetObject then leaves it out.
 */
@Testcontainers
@SpringBootTest(properties = {
        "spring.grpc.server.port=0",
        "coordinator.node-rpc-timeout=500ms",
        "coordinator.liveness-sweep=0",
})
class BadReplicaReportIT {

    @Container
    @ServiceConnection
    static PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:18");

    static final FakeNodes nodes = new FakeNodes(4);

    @DynamicPropertySource
    static void registerNodes(DynamicPropertyRegistry registry) {
        registry.add("coordinator.nodes", nodes::nodesProperty);
    }

    private final MetadataService metadata;
    private final ReplicaHealthService health;
    private final JdbcClient jdbc;
    private final LivenessService liveness;

    @Autowired
    BadReplicaReportIT(MetadataService metadata, ReplicaHealthService health, JdbcClient jdbc,
            LivenessService liveness) {
        this.metadata = metadata;
        this.health = health;
        this.jdbc = jdbc;
        this.liveness = liveness;
    }

    @BeforeEach
    void clear() {
        jdbc.sql("TRUNCATE keys, chunk_replicas, chunk_placements, chunks, objects").update();
        nodes.reset();
        for (String n : List.of("node1", "node2", "node3", "node4")) {
            liveness.heartbeat(n, "test-instance");
        }
    }

    @AfterEach
    void releaseHangs() {
        nodes.reset();
    }

    @Test
    void corruptionConfirmedByTheNodeMarksTheReplicaBad() {
        ChunkPlacement c = commitOneChunk("corrupt");
        String node = nodeIds(c).getFirst();
        nodes.get(node).verifyAs.put(c.chunkId().toString(), VerifyChunkResponse.State.STATE_CORRUPT);

        assertThat(report(c, node)).isEqualTo(Verdict.VERDICT_MARKED_BAD);

        assertThat(stateOf(c.chunkId(), node)).isEqualTo("BAD");
        assertThat(reasonOf(c.chunkId(), node)).startsWith("corrupt: fake: data hashes wrong");
        assertThat(jdbc.sql("SELECT marked_bad_at IS NOT NULL FROM chunk_replicas WHERE chunk_id = :c AND node_id = :n")
                .param("c", c.chunkId()).param("n", node).query(Boolean.class).single()).isTrue();
        assertThat(nodes.get(node).verifyCalls.get()).isEqualTo(1);
    }

    @Test
    void missingConfirmedByTheNodeMarksTheReplicaBad() {
        ChunkPlacement c = commitOneChunk("missing");
        String node = nodeIds(c).getFirst();
        nodes.get(node).verifyAs.put(c.chunkId().toString(), VerifyChunkResponse.State.STATE_MISSING);

        assertThat(report(c, node)).isEqualTo(Verdict.VERDICT_MARKED_BAD);
        assertThat(stateOf(c.chunkId(), node)).isEqualTo("BAD");
        assertThat(reasonOf(c.chunkId(), node)).isEqualTo("missing");
    }

    /** Mutation: mark BAD on the client's word, without asking the node. */
    @Test
    void reportContradictedByTheNodeMarksNothing() {
        ChunkPlacement c = commitOneChunk("contradicted");
        String node = nodeIds(c).getFirst(); // holds the chunk: VerifyChunk says HEALTHY

        assertThat(report(c, node)).isEqualTo(Verdict.VERDICT_CONTRADICTED);
        assertThat(stateOf(c.chunkId(), node)).isEqualTo("HEALTHY");
        assertThat(nodes.get(node).verifyCalls.get()).as("the node was asked").isEqualTo(1);
    }

    @Test
    void noAnswerFromTheNodeMarksNothing() {
        ChunkPlacement c = commitOneChunk("silent");
        String node = nodeIds(c).getFirst();
        nodes.get(node).verifyAs.put(c.chunkId().toString(), VerifyChunkResponse.State.STATE_CORRUPT);
        nodes.get(node).mode = Mode.FAIL;

        assertThat(report(c, node)).isEqualTo(Verdict.VERDICT_UNCONFIRMED);
        assertThat(stateOf(c.chunkId(), node)).isEqualTo("HEALTHY");
    }

    /** Mutation: accept a VerifyChunk response from the wrong node. */
    @Test
    void confirmationFromTheWrongNodeMarksNothing() {
        ChunkPlacement c = commitOneChunk("misdirected");
        List<String> placed = nodeIds(c);
        String node = placed.getFirst();
        // The call meant for node answers as another node, and claims corruption.
        nodes.get(node).answerAs = placed.get(1);
        nodes.get(node).verifyAs.put(c.chunkId().toString(), VerifyChunkResponse.State.STATE_CORRUPT);

        assertThat(report(c, node)).isEqualTo(Verdict.VERDICT_UNCONFIRMED);
        assertThat(stateOf(c.chunkId(), node)).isEqualTo("HEALTHY");
        assertThat(stateOf(c.chunkId(), placed.get(1))).isEqualTo("HEALTHY");
    }

    @Test
    void reportForAnUnrecordedReplicaIsNotAReplica() {
        ChunkPlacement c = commitOneChunk("stranger");
        String outsider = List.of("node1", "node2", "node3", "node4").stream()
                .filter(n -> !nodeIds(c).contains(n)).findFirst().orElseThrow();

        assertThat(report(c, outsider)).isEqualTo(Verdict.VERDICT_NOT_A_REPLICA);
        assertThat(nodes.get(outsider).verifyCalls.get()).as("no node call for a non-replica").isZero();
    }

    @Test
    void reportForAnAlreadyBadReplicaChangesNothing() {
        ChunkPlacement c = commitOneChunk("twice");
        String node = nodeIds(c).getFirst();
        nodes.get(node).verifyAs.put(c.chunkId().toString(), VerifyChunkResponse.State.STATE_CORRUPT);
        assertThat(report(c, node)).isEqualTo(Verdict.VERDICT_MARKED_BAD);
        String reason = reasonOf(c.chunkId(), node);

        assertThat(report(c, node)).isEqualTo(Verdict.VERDICT_ALREADY_BAD);
        assertThat(reasonOf(c.chunkId(), node)).isEqualTo(reason);
        assertThat(nodes.get(node).verifyCalls.get()).as("not asked again").isEqualTo(1);
    }

    @Test
    void concurrentReportsMarkExactlyOnce() throws Exception {
        ChunkPlacement c = commitOneChunk("race");
        String node = nodeIds(c).getFirst();
        nodes.get(node).verifyAs.put(c.chunkId().toString(), VerifyChunkResponse.State.STATE_CORRUPT);

        int reporters = 8;
        CountDownLatch start = new CountDownLatch(1);
        List<Future<Verdict>> futures = new ArrayList<>();
        try (ExecutorService pool = Executors.newFixedThreadPool(reporters)) {
            for (int i = 0; i < reporters; i++) {
                futures.add(pool.submit(() -> {
                    start.await();
                    return report(c, node);
                }));
            }
            start.countDown();
            List<Verdict> verdicts = new ArrayList<>();
            for (Future<Verdict> f : futures) {
                verdicts.add(f.get());
            }
            assertThat(verdicts).filteredOn(v -> v == Verdict.VERDICT_MARKED_BAD).hasSize(1);
            assertThat(verdicts).filteredOn(v -> v != Verdict.VERDICT_MARKED_BAD).containsOnly(Verdict.VERDICT_ALREADY_BAD);
        }
    }

    /** Mutation: GetObject ignores the replica state. */
    @Test
    void getObjectLeavesOutBadReplicas() {
        ChunkPlacement c = commitOneChunk("excluded");
        String bad = nodeIds(c).getFirst();
        markBad(c, bad);

        List<String> served = metadata.getObject("excluded").orElseThrow().chunks().getFirst().replicas().stream()
                .map(Node::nodeId).toList();
        assertThat(served).doesNotContain(bad).containsExactlyInAnyOrderElementsOf(nodeIds(c).subList(1, 3));
    }

    /** Mutation: GetObject returns a chunk with no healthy replica instead of DATA_LOSS. */
    @Test
    void getObjectFailsWithDataLossWhenAChunkHasNoHealthyReplica() {
        ChunkPlacement c = commitOneChunk("lost");
        for (String node : nodeIds(c)) {
            markBad(c, node);
        }

        assertThatThrownBy(() -> metadata.getObject("lost"))
                .isInstanceOfSatisfying(RequestException.class,
                        e -> assertThat(e.code()).isEqualTo(Status.Code.DATA_LOSS))
                .hasMessageContaining("chunk 0 (" + c.chunkId() + ") has no healthy replicas")
                .hasMessageContaining(nodeIds(c).get(0) + ": corrupt: fake")
                .hasMessageContaining(nodeIds(c).get(1) + ": corrupt: fake")
                .hasMessageContaining(nodeIds(c).get(2) + ": corrupt: fake");
    }

    private void markBad(ChunkPlacement c, String node) {
        nodes.get(node).verifyAs.put(c.chunkId().toString(), VerifyChunkResponse.State.STATE_CORRUPT);
        assertThat(report(c, node)).isEqualTo(Verdict.VERDICT_MARKED_BAD);
    }

    private Verdict report(ChunkPlacement c, String node) {
        return health.reportBadReplica(c.chunkId(), node, "REASON_DATA_LOSS", "test report");
    }

    /** Commits a 10-byte object held by all three placed nodes and returns its chunk. */
    private ChunkPlacement commitOneChunk(String key) {
        UploadPlan plan = metadata.initiateUpload(key, 10);
        ChunkPlacement c = plan.chunks().getFirst();
        byte[] sha = sha(c.chunkId());
        for (String n : nodeIds(c)) {
            nodes.get(n).held.put(c.chunkId().toString(), new Held(ChunkStat.State.STATE_FOUND, 10, sha));
        }
        metadata.completeUpload(plan.objectId(), key, 10, List.of(new CompletedChunk(c.chunkId(), 0, 10, sha)));
        return c;
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

    private String stateOf(UUID chunk, String node) {
        return jdbc.sql("SELECT state FROM chunk_replicas WHERE chunk_id = :c AND node_id = :n")
                .param("c", chunk).param("n", node).query(String.class).single();
    }

    private String reasonOf(UUID chunk, String node) {
        return jdbc.sql("SELECT bad_reason FROM chunk_replicas WHERE chunk_id = :c AND node_id = :n")
                .param("c", chunk).param("n", node).query(String.class).single();
    }
}
