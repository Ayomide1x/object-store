package io.github.ayomide1x.objectstore.coordinator;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

import io.github.ayomide1x.objectstore.coordinator.FakeNodes.Held;
import io.github.ayomide1x.objectstore.coordinator.FakeNodes.PushMode;
import io.github.ayomide1x.objectstore.coordinator.LivenessTrackerTest.FakeTicker;
import io.github.ayomide1x.objectstore.coordinator.MetadataService.ChunkPlacement;
import io.github.ayomide1x.objectstore.coordinator.MetadataService.CompletedChunk;
import io.github.ayomide1x.objectstore.coordinator.MetadataService.UploadPlan;
import io.github.ayomide1x.objectstore.v1.ChunkStat;
import io.github.ayomide1x.objectstore.v1.VerifyChunkResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * The reconciler, against fake nodes and a fake clock. Passes are run by
 * hand; the scheduler and the liveness sweeper are off.
 */
@Testcontainers
@SpringBootTest(properties = {
        "spring.grpc.server.port=0",
        "coordinator.node-rpc-timeout=500ms",
        "coordinator.copy-timeout=2s",
        "coordinator.liveness-sweep=0",
        "coordinator.repair-interval=0",
        "coordinator.gc-interval=0",
})
class RepairIT {

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
    private final RepairService repair;
    private final MetadataRepository repository;
    private final NodeClient nodeClient;
    private final ReplicaHealthService replicaHealth;
    private final CoordinatorProperties properties;
    private final JdbcClient jdbc;
    private final LivenessTracker tracker;
    private final TransactionTemplate tx;

    @Autowired
    RepairIT(MetadataService metadata, LivenessService liveness, RepairService repair, MetadataRepository repository,
            NodeClient nodeClient, ReplicaHealthService replicaHealth, CoordinatorProperties properties, JdbcClient jdbc,
            LivenessTracker tracker, TransactionTemplate tx) {
        this.tx = tx;
        this.metadata = metadata;
        this.liveness = liveness;
        this.repair = repair;
        this.repository = repository;
        this.nodeClient = nodeClient;
        this.replicaHealth = replicaHealth;
        this.properties = properties;
        this.jdbc = jdbc;
        this.tracker = tracker;
    }

    @BeforeEach
    void allAlive() {
        jdbc.sql("TRUNCATE keys, pending_deletions, chunk_replicas, chunk_placements, chunks, objects, repair_passes, gc_passes").update();
        nodes.reset();
        for (int i = 0; i < 2; i++) {
            ALL.forEach(n -> liveness.heartbeat(n, "test-instance"));
        }
    }

    @AfterEach
    void releaseHangs() {
        nodes.reset();
    }

    /** An object of one 10-byte chunk, committed with replicas on exactly the given placed positions. */
    private record One(UUID chunkId, List<String> placed, String free) {
    }

    private One commitOneChunk(String key, int... holders) {
        UploadPlan plan = metadata.initiateUpload(key, 10);
        ChunkPlacement c = plan.chunks().getFirst();
        List<String> placed = c.nodes().stream().map(Node::nodeId).toList();
        for (int h : holders) {
            nodes.get(placed.get(h)).held.put(c.chunkId().toString(), new Held(ChunkStat.State.STATE_FOUND, 10, sha(c.chunkId())));
        }
        metadata.completeUpload(plan.objectId(), key, 10, List.of(new CompletedChunk(c.chunkId(), 0, 10, sha(c.chunkId()))));
        String free = ALL.stream().filter(n -> !placed.contains(n)).findFirst().orElseThrow();
        return new One(c.chunkId(), placed, free);
    }

    private One commitOneChunk(String key) {
        return commitOneChunk(key, 0, 1, 2);
    }

    /** Makes node DEAD, then lets extra time pass, keeping every other node ALIVE. */
    private void dead(String node, Duration extra) {
        clock.advance(Duration.ofSeconds(16));
        ALL.stream().filter(n -> !n.equals(node)).forEach(n -> liveness.heartbeat(n, "test-instance"));
        liveness.sweep();
        assertThat(liveness.stateOf(node)).isEqualTo(LivenessTracker.State.DEAD);
        clock.advance(extra);
        ALL.stream().filter(n -> !n.equals(node)).forEach(n -> liveness.heartbeat(n, "test-instance"));
        liveness.sweep();
    }

    private void deadPastGrace(String node) {
        dead(node, properties.repairGrace().plusSeconds(1));
    }

    private List<String> healthyOn(UUID chunk) {
        return jdbc.sql("SELECT node_id FROM chunk_replicas WHERE chunk_id = :c AND state = 'HEALTHY' ORDER BY node_id")
                .param("c", chunk).query(String.class).list();
    }

    private String rowOf(UUID chunk, String node) {
        return jdbc.sql("SELECT state || ':' || origin FROM chunk_replicas WHERE chunk_id = :c AND node_id = :n")
                .param("c", chunk).param("n", node).query(String.class).optional().orElse("none");
    }

    private void markBad(UUID chunk, String node) {
        jdbc.sql("UPDATE chunk_replicas SET state = 'BAD', bad_reason = 'test', marked_bad_at = now() WHERE chunk_id = :c AND node_id = :n")
                .param("c", chunk).param("n", node).update();
    }

    /** Mutation: count replicas on DEAD nodes as healthy. */
    @Test
    void aDeadNodesReplicaIsCopiedFromAHealthySourceToAFreeNode() {
        One c = commitOneChunk("lost");
        String deadNode = c.placed().get(0);
        deadPastGrace(deadNode);

        RepairService.PassResult result = repair.runPass();

        assertThat(nodes.pushes).hasSize(1);
        FakeNodes.Push p = nodes.pushes.getFirst();
        assertThat(p.chunkId()).isEqualTo(c.chunkId().toString());
        assertThat(p.source()).isIn(c.placed().get(1), c.placed().get(2));
        assertThat(p.target()).isEqualTo(c.free());
        assertThat(rowOf(c.chunkId(), c.free())).isEqualTo("HEALTHY:repair");
        assertThat(result.recorded()).isEqualTo(1);
    }

    /** Mutation: no grace period (repair as soon as a node is DEAD). */
    @Test
    void nothingIsCopiedWithinTheGracePeriod() {
        One c = commitOneChunk("grace");
        String deadNode = c.placed().get(0);
        dead(deadNode, properties.repairGrace().minusSeconds(1));
        repair.runPass();
        assertThat(nodes.pushes).as("DEAD for less than the grace period").isEmpty();

        clock.advance(Duration.ofSeconds(2));
        ALL.stream().filter(n -> !n.equals(deadNode)).forEach(n -> liveness.heartbeat(n, "test-instance"));
        repair.runPass();
        assertThat(nodes.pushes).as("past the grace period").hasSize(1);
    }

    /** Mutation: act while a node is UNKNOWN. */
    @Test
    void aPassDoesNothingWhileAnyNodeIsUnknown() {
        One c = commitOneChunk("born-with-two", 0, 1); // under-replicated, with a free ALIVE target
        // A coordinator that has just restarted: node4 has not beaten yet.
        LivenessTracker fresh = new LivenessTracker(clock, properties);
        List.of("node1", "node2", "node3").forEach(n -> fresh.beat(n, "test-instance"));
        RepairService justRestarted = new RepairService(repository, nodeClient, fresh, replicaHealth, properties, tx);

        RepairService.PassResult result = justRestarted.runPass();

        assertThat(result.skippedReason()).contains("UNKNOWN").contains("node4");
        assertThat(nodes.pushes).isEmpty();
        assertThat(healthyOn(c.chunkId())).hasSize(2);
    }

    @Test
    void aSecondPassRightAfterACompleteOneDoesNothing() {
        commitOneChunk("twice-a");
        commitOneChunk("twice-b");
        deadPastGrace("node2");
        repair.runPass();
        int afterFirst = nodes.pushes.size();

        RepairService.PassResult second = repair.runPass();

        assertThat(nodes.pushes).hasSize(afterFirst);
        assertThat(second.copiesStarted()).isZero();
        assertThat(jdbc.sql("SELECT copies_started FROM repair_passes ORDER BY pass_id DESC LIMIT 1")
                .query(Integer.class).single()).isZero();
    }

    /** Mutation: resolve TARGET_HAS_CHUNK by pushing again. */
    @Test
    void alreadyExistsWithAValidCopyIsRecordedNotRetried() {
        One c = commitOneChunk("found");
        deadPastGrace(c.placed().get(0));
        c.placed().forEach(n -> nodes.get(n).pushMode = PushMode.HAS_CHUNK);
        nodes.get(c.free()).held.put(c.chunkId().toString(), new Held(ChunkStat.State.STATE_FOUND, 10, sha(c.chunkId())));

        repair.runPass();

        assertThat(nodes.pushes).as("one push, then the target's own report decides").hasSize(1);
        assertThat(rowOf(c.chunkId(), c.free())).isEqualTo("HEALTHY:repair-found");
    }

    @Test
    void alreadyExistsWithAnInvalidFileRecordsABadRowAndThatNodeIsNotTargetedAgain() {
        One c = commitOneChunk("junk");
        deadPastGrace(c.placed().get(0));
        c.placed().forEach(n -> nodes.get(n).pushMode = PushMode.HAS_CHUNK);
        nodes.get(c.free()).held.put(c.chunkId().toString(), new Held(ChunkStat.State.STATE_BAD_HEADER, 0, null));

        repair.runPass();
        assertThat(rowOf(c.chunkId(), c.free())).isEqualTo("BAD:repair");

        nodes.pushes.clear();
        repair.runPass();
        assertThat(nodes.pushes).as("the BAD-row node is not a target").isEmpty();
    }

    /** Mutation: record the replica without the target's confirmation. */
    @Test
    void aFailedCopyRecordsNothing() {
        One c = commitOneChunk("failed");
        deadPastGrace(c.placed().get(0));
        c.placed().forEach(n -> nodes.get(n).pushMode = PushMode.TARGET_FAIL);
        repair.runPass();
        assertThat(rowOf(c.chunkId(), c.free())).isEqualTo("none");

        // The source says COPIED, but the target holds nothing: its word decides.
        c.placed().forEach(n -> nodes.get(n).pushMode = PushMode.LIE);
        repair.runPass();
        assertThat(rowOf(c.chunkId(), c.free())).isEqualTo("none");
    }

    @Test
    void aSourceWithDataLossIsConfirmedAndMarkedAndTheNextSourceIsUsed() {
        One c = commitOneChunk("bad-source");
        deadPastGrace(c.placed().get(0));
        String first = c.placed().subList(1, 3).stream().sorted().findFirst().orElseThrow();
        nodes.get(first).pushMode = PushMode.DATA_LOSS;
        nodes.get(first).verifyAs.put(c.chunkId().toString(), VerifyChunkResponse.State.STATE_CORRUPT);

        repair.runPass();

        assertThat(rowOf(c.chunkId(), first)).isEqualTo("BAD:upload");
        assertThat(nodes.get(first).verifyCalls.get()).as("the source was asked to confirm").isEqualTo(1);
        assertThat(rowOf(c.chunkId(), c.free())).isEqualTo("HEALTHY:repair");
    }

    /** Mutation: target a node with a BAD row. */
    @Test
    void aNodeWithABadRowIsNeverATarget() {
        // A layout where the BAD-row node sorts before the free node, so only
        // the exclusion (not the tie-break) keeps it from being chosen.
        One c = null;
        String badNode = null;
        for (int i = 0; i < 40 && badNode == null; i++) {
            One candidate = commitOneChunk("bad-target-" + i);
            badNode = candidate.placed().stream().filter(n -> n.compareTo(candidate.free()) < 0).findFirst().orElse(null);
            c = candidate;
            if (badNode == null) {
                jdbc.sql("TRUNCATE keys, pending_deletions, chunk_replicas, chunk_placements, chunks, objects").update();
            }
        }
        assertThat(badNode).as("a usable layout").isNotNull();
        markBad(c.chunkId(), badNode);

        repair.runPass();

        assertThat(nodes.pushes).hasSize(1);
        assertThat(nodes.pushes.getFirst().target()).isEqualTo(c.free()).isNotEqualTo(badNode);
    }

    /** Mutation: copy from a BAD replica. */
    @Test
    void aBadReplicaIsNeverASource() {
        One c = commitOneChunk("bad-source-row");
        // The BAD replica sorts first among the sources, so only the rule keeps it out.
        String badNode = c.placed().stream().sorted().findFirst().orElseThrow();
        markBad(c.chunkId(), badNode);

        repair.runPass();

        assertThat(nodes.pushes).hasSize(1);
        assertThat(nodes.pushes.getFirst().source()).isNotEqualTo(badNode);
    }

    /**
     * The caps bound copies in flight, not copies per pass: one pass repairs
     * every chunk, refilling slots as copies finish. Mutations: no concurrency
     * bound; the caps as a per-pass budget.
     */
    @Test
    void copiesAreBoundedInFlightAndOnePassRepairsEverything() {
        List<One> chunks = new java.util.ArrayList<>();
        for (int i = 0; i < 12; i++) {
            chunks.add(commitOneChunk("many-" + i));
        }
        deadPastGrace("node4");
        nodes.pushDelayMs = 100;

        repair.runPass();

        long needed = chunks.stream().filter(c -> c.placed().contains("node4")).count();
        assertThat(nodes.pushes).as("one copy per chunk that had a replica on node4").hasSize((int) needed);
        assertThat(needed).as("more copies than fit at once").isGreaterThan(3);
        for (One c : chunks) {
            assertThat(healthyOn(c.chunkId()).stream().filter(x -> !x.equals("node4")).count())
                    .as("chunk %s after one pass", c.chunkId()).isEqualTo(3);
        }
        assertThat(nodes.maxOnAnyNode()).as("max copies at once on one node").isLessThanOrEqualTo(properties.repairMaxPerNode());
        assertThat(nodes.maxInFlight()).as("max copies at once").isLessThanOrEqualTo(properties.repairMaxInflight());
    }

    @Test
    void chunksDownToOneReplicaAreRepairedFirst() {
        One older = commitOneChunk("two-left", 0, 1);   // counted 2, and older
        One newer = commitOneChunk("one-left", 0, 1, 2);
        markBad(newer.chunkId(), newer.placed().get(1));
        markBad(newer.chunkId(), newer.placed().get(2)); // counted 1
        CoordinatorProperties oneAtATime = new CoordinatorProperties(properties.nodes(), properties.nodeRpcTimeout(),
                properties.suspectAfter(), properties.deadAfter(), properties.recoveryBeats(), properties.livenessSweep(),
                properties.repairGrace(), Duration.ZERO, 1, 1, properties.copyTimeout(), properties.repairPassRetention(),
                properties.gcInterval(), properties.gcReadGrace(), properties.gcPendingTimeout(), properties.gcBatch(),
                properties.gcMaxInflight(), properties.gcMaxPerNode(), properties.gcPassRetention());
        RepairService capped = new RepairService(repository, nodeClient, tracker, replicaHealth, oneAtATime, tx);

        capped.runPass();

        // One copy at a time, so the order of pushes is the priority order: the
        // 1-replica chunk goes first although the 2-replica chunk is older.
        assertThat(nodes.pushes).extracting(FakeNodes.Push::chunkId).first()
                .as("first copy").isEqualTo(newer.chunkId().toString());
        assertThat(nodes.pushes).extracting(FakeNodes.Push::chunkId).contains(older.chunkId().toString());
        assertThat(nodes.maxInFlight()).isEqualTo(1);
    }

    @Test
    void aChunkBornWithTwoReplicasIsToppedUp() {
        One c = commitOneChunk("born-two", 0, 1);
        assertThat(healthyOn(c.chunkId())).hasSize(2);
        repair.runPass();
        assertThat(healthyOn(c.chunkId())).hasSize(3);
    }

    @Test
    void supersededAndPendingObjectsAreNotRepaired() {
        commitOneChunk("over", 0, 1);     // v1: under-replicated...
        commitOneChunk("over");           // ...then superseded by a full v2
        metadata.initiateUpload("pending", 10); // never completed
        repair.runPass();
        assertThat(nodes.pushes).isEmpty();
    }

    /** Mutation: repair_passes is never pruned. */
    @Test
    void oldRepairPassRowsArePrunedAndRecentOnesKept() {
        jdbc.sql("INSERT INTO repair_passes (started_at, finished_at) VALUES (now() - interval '25 hours', now() - interval '25 hours')").update();
        jdbc.sql("INSERT INTO repair_passes (started_at, finished_at) VALUES (now() - interval '1 hour', now() - interval '1 hour')").update();

        repair.runPass();

        assertThat(jdbc.sql("SELECT count(*) FROM repair_passes WHERE started_at < now() - interval '24 hours'")
                .query(Long.class).single()).as("older than the 24 h retention").isZero();
        assertThat(jdbc.sql("SELECT count(*) FROM repair_passes").query(Long.class).single())
                .as("the 1-hour-old row and this pass's").isEqualTo(2);
    }

    private static byte[] sha(UUID id) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(id.toString().getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
