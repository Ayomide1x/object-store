package io.github.ayomide1x.objectstore.coordinator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import io.github.ayomide1x.objectstore.coordinator.FakeNodes.Held;
import io.github.ayomide1x.objectstore.coordinator.FakeNodes.Mode;
import io.github.ayomide1x.objectstore.coordinator.LivenessTrackerTest.FakeTicker;
import io.github.ayomide1x.objectstore.coordinator.MetadataService.ChunkPlacement;
import io.github.ayomide1x.objectstore.coordinator.MetadataService.CompletedChunk;
import io.github.ayomide1x.objectstore.coordinator.MetadataService.UploadPlan;
import io.github.ayomide1x.objectstore.v1.ChunkStat;
import io.grpc.Status;
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
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Garbage collection, against fake nodes, the fake liveness ticker and a
 * settable wall clock for the grace periods. Passes are run by hand; every
 * scheduler and the liveness sweeper are off.
 */
@Testcontainers
@SpringBootTest(properties = {
        "spring.grpc.server.port=0",
        "coordinator.node-rpc-timeout=500ms",
        "coordinator.copy-timeout=2s",
        "coordinator.liveness-sweep=0",
        "coordinator.repair-interval=0",
        "coordinator.gc-interval=0",
        "coordinator.gc-read-grace=60s",
        "coordinator.gc-pending-timeout=120s",
        "coordinator.gc-batch=4",
        "coordinator.gc-max-inflight=2",
        "coordinator.gc-max-per-node=1",
})
class GcIT {

    static final List<String> ALL = List.of("node1", "node2", "node3", "node4");
    static final Duration GRACE = Duration.ofSeconds(60);
    static final Duration PENDING_TIMEOUT = Duration.ofSeconds(120);
    static final long CHUNK = MetadataService.CHUNK_SIZE;

    @Container
    @ServiceConnection
    static PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:18");

    static final FakeNodes nodes = new FakeNodes(4);
    static final FakeTicker ticker = new FakeTicker();
    static final MutableClock wall = new MutableClock();

    /** A wall clock tests move forward by hand. */
    static final class MutableClock extends Clock {
        volatile Instant now = Instant.now();

        @Override
        public Instant instant() {
            return now;
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        void advance(Duration d) {
            now = now.plus(d);
        }
    }

    @DynamicPropertySource
    static void registerNodes(DynamicPropertyRegistry registry) {
        registry.add("coordinator.nodes", nodes::nodesProperty);
    }

    @TestConfiguration
    static class Clocks {
        @Bean
        @Primary
        Ticker fakeTicker() {
            return ticker;
        }

        @Bean
        @Primary
        Clock fakeWallClock() {
            return wall;
        }
    }

    private final MetadataService metadata;
    private final LivenessService liveness;
    private final GcService gc;
    private final RepairService repair;
    private final JdbcClient jdbc;

    @Autowired
    GcIT(MetadataService metadata, LivenessService liveness, GcService gc, RepairService repair, JdbcClient jdbc) {
        this.metadata = metadata;
        this.liveness = liveness;
        this.gc = gc;
        this.repair = repair;
        this.jdbc = jdbc;
    }

    @BeforeEach
    void allAlive() {
        jdbc.sql("TRUNCATE keys, pending_deletions, chunk_replicas, chunk_placements, chunks, objects, repair_passes, gc_passes")
                .update();
        nodes.reset();
        // Marks are made with the database clock; keep the wall clock at real time.
        wall.now = Instant.now();
        ticker.advance(Duration.ofSeconds(60)); // past any earlier test's DEAD node
        for (int i = 0; i < 2; i++) {
            ALL.forEach(n -> liveness.heartbeat(n, "test-instance"));
        }
        ALL.forEach(n -> assertThat(liveness.stateOf(n)).isEqualTo(LivenessTracker.State.ALIVE));
    }

    @AfterEach
    void releaseHangs() {
        nodes.reset();
    }

    /** An upload of one 10-byte chunk, its bytes on the given placed positions. */
    private record Upload(UUID objectId, String key, UUID chunkId, List<String> placed, String free,
            List<CompletedChunk> completed) {
    }

    private Upload initiate(String key, int... holders) {
        UploadPlan plan = metadata.initiateUpload(key, 10);
        ChunkPlacement c = plan.chunks().getFirst();
        List<String> placed = c.nodes().stream().map(Node::nodeId).toList();
        for (int h : holders) {
            nodes.get(placed.get(h)).held.put(c.chunkId().toString(), new Held(ChunkStat.State.STATE_FOUND, 10, sha(c.chunkId())));
        }
        String free = ALL.stream().filter(n -> !placed.contains(n)).findFirst().orElseThrow();
        return new Upload(plan.objectId(), key, c.chunkId(), placed, free,
                List.of(new CompletedChunk(c.chunkId(), 0, 10, sha(c.chunkId()))));
    }

    private void complete(Upload u) {
        metadata.completeUpload(u.objectId(), u.key(), 10, u.completed());
    }

    private Upload commit(String key, int... holders) {
        Upload u = initiate(key, holders);
        complete(u);
        return u;
    }

    private Upload commit(String key) {
        return commit(key, 0, 1, 2);
    }

    private String statusOf(UUID objectId) {
        return jdbc.sql("SELECT status FROM objects WHERE object_id = :id").param("id", objectId)
                .query(String.class).optional().orElse("gone");
    }

    private long count(String sql, UUID id) {
        return jdbc.sql(sql).param("id", id).query(Long.class).single();
    }

    private long rowsOf(UUID chunk) {
        return count("SELECT count(*) FROM chunk_replicas WHERE chunk_id = :id", chunk);
    }

    private List<String> taskNodes(UUID chunk) {
        return jdbc.sql("SELECT node_id FROM pending_deletions WHERE chunk_id = :c ORDER BY node_id")
                .param("c", chunk).query(String.class).list();
    }

    private List<FakeNodes.Delete> deletesOf(UUID chunk) {
        return nodes.deletes.stream().filter(d -> d.chunkId().equals(chunk.toString())).toList();
    }

    private boolean holds(String node, UUID chunk) {
        return nodes.get(node).held.containsKey(chunk.toString());
    }

    private void markBad(UUID chunk, String node) {
        jdbc.sql("UPDATE chunk_replicas SET state = 'BAD', bad_reason = 'test', marked_bad_at = clock_timestamp() "
                + "WHERE chunk_id = :c AND node_id = :n").param("c", chunk).param("n", node).update();
    }

    /** Makes node DEAD (others kept ALIVE), then lets extra liveness time pass. */
    private void dead(String node, Duration extra) {
        ticker.advance(Duration.ofSeconds(16));
        ALL.stream().filter(n -> !n.equals(node)).forEach(n -> liveness.heartbeat(n, "test-instance"));
        liveness.sweep();
        assertThat(liveness.stateOf(node)).isEqualTo(LivenessTracker.State.DEAD);
        ticker.advance(extra);
        ALL.stream().filter(n -> !n.equals(node)).forEach(n -> liveness.heartbeat(n, "test-instance"));
        liveness.sweep();
    }

    private void backAlive(String node) {
        liveness.heartbeat(node, "test-instance");
        liveness.heartbeat(node, "test-instance");
        assertThat(liveness.stateOf(node)).isEqualTo(LivenessTracker.State.ALIVE);
    }

    /** Mutation G4: no read grace for superseded objects. */
    @Test
    void aSupersededObjectKeepsItsFilesForTheReadGraceThenEverythingGoes() {
        Upload old = commit("doc");
        Upload current = commit("doc");

        GcService.PassResult first = gc.runPass();
        assertThat(first.superseded()).isEqualTo(1);
        assertThat(statusOf(old.objectId())).isEqualTo("SUPERSEDED");
        assertThat(nodes.deletes).as("superseded, but within the read grace").isEmpty();

        wall.advance(GRACE.minusSeconds(1));
        gc.runPass();
        assertThat(nodes.deletes).as("one second before the grace ends").isEmpty();
        old.placed().forEach(n -> assertThat(holds(n, old.chunkId())).isTrue());

        wall.advance(Duration.ofSeconds(2));
        GcService.PassResult after = gc.runPass();
        assertThat(deletesOf(old.chunkId())).extracting(FakeNodes.Delete::node)
                .containsExactlyInAnyOrderElementsOf(old.placed());
        assertThat(deletesOf(old.chunkId())).allMatch(FakeNodes.Delete::tombstone);
        old.placed().forEach(n -> assertThat(holds(n, old.chunkId())).isFalse());
        assertThat(statusOf(old.objectId())).as("metadata removed once every node confirmed").isEqualTo("gone");
        assertThat(count("SELECT count(*) FROM chunks WHERE object_id = :id", old.objectId())).isZero();
        assertThat(after.objectsRemoved()).isEqualTo(1);

        // The current version is untouched and still reads back.
        assertThat(deletesOf(current.chunkId())).isEmpty();
        assertThat(metadata.getObject("doc").orElseThrow().objectId()).isEqualTo(current.objectId());
        assertThat(metadata.getObject("doc").orElseThrow().chunks().getFirst().replicas()).hasSize(3);
    }

    /** Mutation G5: supersede detection ignores keys, so the current version is retired. */
    @Test
    void theCurrentVersionIsNeverDeleted() {
        Upload u = commit("keep");
        wall.advance(Duration.ofDays(2));

        gc.runPass();
        gc.runPass();

        assertThat(statusOf(u.objectId())).isEqualTo("COMMITTED");
        assertThat(nodes.deletes).isEmpty();
        assertThat(rowsOf(u.chunkId())).isEqualTo(3);
        assertThat(metadata.getObject("keep")).isPresent();
    }

    @Test
    void rmMakesTheKeyNotFoundAtOnceAndItsFilesGoAfterTheGrace() {
        Upload u = commit("gone");

        UUID removed = metadata.deleteObject("gone");

        assertThat(removed).isEqualTo(u.objectId());
        assertThat(metadata.getObject("gone")).isEmpty();
        assertThat(metadata.statObject("gone")).isEmpty();
        assertThat(statusOf(u.objectId())).isEqualTo("DELETED");
        assertThatThrownBy(() -> metadata.deleteObject("gone")).isInstanceOfSatisfying(RequestException.class,
                e -> assertThat(e.code()).isEqualTo(Status.Code.NOT_FOUND));
        assertThatThrownBy(() -> metadata.deleteObject("never-existed")).isInstanceOfSatisfying(RequestException.class,
                e -> assertThat(e.code()).isEqualTo(Status.Code.NOT_FOUND));

        gc.runPass();
        assertThat(nodes.deletes).as("within the grace").isEmpty();

        wall.advance(GRACE.plusSeconds(1));
        gc.runPass();
        u.placed().forEach(n -> assertThat(holds(n, u.chunkId())).isFalse());
        assertThat(statusOf(u.objectId())).isEqualTo("gone");
    }

    /** Mutation G1: files deleted before the object is retired (an unreferenced PENDING upload). */
    @Test
    void anUploadInProgressIsNeverTouched() {
        Upload u = initiate("slow", 0, 1, 2);
        wall.advance(GRACE.plusSeconds(10)); // past the read grace, within the pending timeout

        gc.runPass();

        assertThat(nodes.deletes).isEmpty();
        assertThat(statusOf(u.objectId())).isEqualTo("PENDING");
        complete(u);
        assertThat(statusOf(u.objectId())).isEqualTo("COMMITTED");
    }

    @Test
    void anUploadPendingPastTheTimeoutIsAbandonedAndItsFilesTombstoned() {
        Upload u = initiate("lost", 0, 1); // the client wrote two replicas, then died
        wall.advance(PENDING_TIMEOUT.minusSeconds(1));
        gc.runPass();
        assertThat(statusOf(u.objectId())).as("within the timeout").isEqualTo("PENDING");

        wall.advance(Duration.ofSeconds(2));
        GcService.PassResult result = gc.runPass();

        assertThat(result.abandoned()).isEqualTo(1);
        assertThat(deletesOf(u.chunkId())).extracting(FakeNodes.Delete::node)
                .as("every placed node, including the one that never got bytes")
                .containsExactlyInAnyOrderElementsOf(u.placed());
        assertThat(deletesOf(u.chunkId())).allMatch(FakeNodes.Delete::tombstone);
        u.placed().forEach(n -> assertThat(nodes.get(n).tombstones).contains(u.chunkId().toString()));
        assertThat(statusOf(u.objectId())).isEqualTo("gone");
        assertThat(count("SELECT count(*) FROM chunk_placements WHERE chunk_id = :id", u.chunkId())).isZero();
    }

    /**
     * The race: the complete has asked every node (phase 2) and is about to
     * commit when GC abandons the upload and deletes its chunks. The commit
     * phase finds ABANDONED under the row lock and fails. Mutation G3: GC
     * deletes a stale PENDING upload's files without the atomic transition.
     */
    @Test
    void aLateCompleteOfAnAbandonedUploadFailsAndNothingCommits() throws Exception {
        Upload u = initiate("race", 0, 1, 2);
        wall.advance(PENDING_TIMEOUT.plusSeconds(1));
        nodes.statReached = new CountDownLatch(3);
        nodes.statGate = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread client = Thread.ofVirtual().start(() -> {
            try {
                complete(u);
            } catch (Throwable t) {
                failure.set(t);
            }
        });
        assertThat(nodes.statReached.await(5, TimeUnit.SECONDS)).as("complete reached phase 2").isTrue();

        GcService.PassResult result = gc.runPass();
        assertThat(result.abandoned()).isEqualTo(1);
        nodes.statGate.countDown(); // phase 2 sees every replica FOUND
        client.join(5000);

        assertThat(failure.get()).isInstanceOfSatisfying(RequestException.class,
                e -> assertThat(e.code()).isEqualTo(Status.Code.FAILED_PRECONDITION));
        assertThat(statusOf(u.objectId())).isNotEqualTo("COMMITTED");
        assertThat(metadata.getObject("race")).isEmpty();
        assertThat(jdbc.sql("SELECT count(*) FROM objects WHERE status = 'COMMITTED'").query(Long.class).single()).isZero();
    }

    @Test
    void aCompleteThatCommitsFirstIsNeverAbandoned() {
        Upload u = initiate("just-in-time", 0, 1, 2);
        wall.advance(PENDING_TIMEOUT.plusSeconds(1));
        complete(u);

        GcService.PassResult result = gc.runPass();

        assertThat(result.abandoned()).isZero();
        assertThat(nodes.deletes).isEmpty();
        assertThat(statusOf(u.objectId())).isEqualTo("COMMITTED");
        assertThat(rowsOf(u.chunkId())).isEqualTo(3);
    }

    /**
     * A retried complete after a lost response: SUPERSEDED and DELETED objects
     * did commit, so it succeeds as for COMMITTED; ABANDONED never did.
     * Mutation G10: a complete of a SUPERSEDED object fails.
     */
    @Test
    void aRetriedCompleteSucceedsForSupersededAndDeletedAndFailsForAbandoned() {
        Upload superseded = commit("retry-s");
        commit("retry-s");
        gc.runPass();
        assertThat(statusOf(superseded.objectId())).isEqualTo("SUPERSEDED");
        complete(superseded);

        Upload deleted = commit("retry-d");
        metadata.deleteObject("retry-d");
        assertThat(statusOf(deleted.objectId())).isEqualTo("DELETED");
        complete(deleted);
        assertThat(metadata.getObject("retry-d")).as("a retry doesn't bring the key back").isEmpty();

        Upload abandoned = initiate("retry-a", 0, 1, 2);
        nodes.get(abandoned.placed().get(0)).mode = Mode.FAIL; // keeps a task, so the row stays
        wall.advance(PENDING_TIMEOUT.plusSeconds(1));
        gc.runPass();
        assertThat(statusOf(abandoned.objectId())).isEqualTo("ABANDONED");
        nodes.get(abandoned.placed().get(0)).mode = Mode.NORMAL;
        assertThatThrownBy(() -> complete(abandoned)).isInstanceOfSatisfying(RequestException.class,
                e -> assertThat(e.code()).isEqualTo(Status.Code.FAILED_PRECONDITION));
    }

    /** Mutation G2: rows removed for a node that failed or answered as another node. */
    @Test
    void rowsAreRemovedOnlyWhenTheNodeItselfConfirms() {
        Upload u = commit("confirm");
        metadata.deleteObject("confirm");
        wall.advance(GRACE.plusSeconds(1));
        String failing = u.placed().get(0);
        String impostor = u.placed().get(1);
        String good = u.placed().get(2);
        nodes.get(failing).mode = Mode.FAIL;
        nodes.get(impostor).answerAs = "node9";

        GcService.PassResult result = gc.runPass();

        assertThat(result.failures()).isEqualTo(2);
        assertThat(taskNodes(u.chunkId())).containsExactlyInAnyOrder(failing, impostor);
        assertThat(jdbc.sql("SELECT node_id FROM chunk_replicas WHERE chunk_id = :c").param("c", u.chunkId())
                .query(String.class).list()).containsExactlyInAnyOrder(failing, impostor);
        assertThat(jdbc.sql("SELECT count(*) FROM pending_deletions WHERE chunk_id = :c AND attempts = 1 AND last_error IS NOT NULL")
                .param("c", u.chunkId()).query(Long.class).single()).isEqualTo(2);
        assertThat(statusOf(u.objectId())).isEqualTo("DELETED");
        assertThat(holds(good, u.chunkId())).isFalse();

        nodes.get(failing).mode = Mode.NORMAL;
        nodes.get(impostor).answerAs = impostor;
        gc.runPass();
        assertThat(taskNodes(u.chunkId())).isEmpty();
        assertThat(statusOf(u.objectId())).isEqualTo("gone");
    }

    /**
     * Mutation G16: a node answering OK with an outcome that is neither deleted
     * nor absent is asked again and again, and the pass never ends.
     */
    @Test
    void anUnclearOutcomeIsAFailureAndThePassEnds() {
        Upload u = commit("unclear");
        metadata.deleteObject("unclear");
        wall.advance(GRACE.plusSeconds(1));
        String unclear = u.placed().get(0);
        nodes.get(unclear).unclearFor.add(u.chunkId().toString());

        GcService.PassResult result = gc.runPass();

        assertThat(nodes.get(unclear).deleteCalls.get()).as("asked once in the pass").isEqualTo(1);
        assertThat(result.failures()).isEqualTo(1);
        assertThat(taskNodes(u.chunkId())).as("the other nodes confirmed").containsExactly(unclear);
        assertThat(jdbc.sql("SELECT last_error FROM pending_deletions WHERE chunk_id = :c AND attempts = 1")
                .param("c", u.chunkId()).query(String.class).single()).isEqualTo("outcome neither deleted nor absent");
        assertThat(holds(unclear, u.chunkId())).isTrue();
        assertThat(statusOf(u.objectId())).isEqualTo("DELETED");

        nodes.get(unclear).unclearFor.clear();
        gc.runPass();
        assertThat(taskNodes(u.chunkId())).isEmpty();
        assertThat(statusOf(u.objectId())).isEqualTo("gone");
    }

    /** Mutation G7: a DEAD node's tasks are dropped instead of waiting. */
    @Test
    void aDeadNodesDeletionsWaitAndAreRetriedWhenItReturns() {
        Upload u = commit("while-dead");
        String down = u.placed().get(0);
        dead(down, Duration.ZERO);
        metadata.deleteObject("while-dead");
        wall.advance(GRACE.plusSeconds(1));

        gc.runPass();

        assertThat(nodes.get(down).deleteCalls.get()).as("a DEAD node is not called").isZero();
        assertThat(taskNodes(u.chunkId())).containsExactly(down);
        assertThat(rowsOf(u.chunkId())).as("its row stays until it confirms").isEqualTo(1);
        assertThat(statusOf(u.objectId())).isEqualTo("DELETED");
        assertThat(holds(down, u.chunkId())).isTrue();

        backAlive(down);
        gc.runPass();

        assertThat(holds(down, u.chunkId())).isFalse();
        assertThat(taskNodes(u.chunkId())).isEmpty();
        assertThat(statusOf(u.objectId())).isEqualTo("gone");
    }

    /**
     * A BAD file of a live chunk is kept for the read grace, then deleted
     * without a tombstone, and the node becomes a repair target again.
     * Mutations G8 (the delete tombstones the chunk) and G12 (no grace).
     */
    @Test
    void aBadFileIsDeletedWithoutATombstoneAndTheNodeIsATargetAgain() {
        Upload u = commit("bad");
        String badNode = u.placed().get(0);
        dead(u.free(), Duration.ZERO); // the only other target is gone
        markBad(u.chunkId(), badNode);
        repair.runPass();
        assertThat(nodes.pushes).as("no target: the free node is DEAD, the BAD node has a row").isEmpty();

        wall.advance(GRACE.minusSeconds(5));
        gc.runPass();
        assertThat(nodes.deletes).as("within the read grace").isEmpty();

        wall.advance(Duration.ofSeconds(10));
        gc.runPass();
        assertThat(deletesOf(u.chunkId())).containsExactly(new FakeNodes.Delete(badNode, u.chunkId().toString(), false));
        assertThat(rowsOf(u.chunkId())).isEqualTo(2);
        assertThat(count("SELECT count(*) FROM chunk_placements WHERE chunk_id = :id", u.chunkId())).isEqualTo(2);
        assertThat(statusOf(u.objectId())).isEqualTo("COMMITTED");

        repair.runPass();
        assertThat(jdbc.sql("SELECT state || ':' || origin FROM chunk_replicas WHERE chunk_id = :c AND node_id = :n")
                .param("c", u.chunkId()).param("n", badNode).query(String.class).optional())
                .as("repaired onto the node whose BAD file was deleted").contains("HEALTHY:repair");
    }

    /**
     * A BAD file is deleted only while 2 HEALTHY replicas count, by repair's
     * rule: a HEALTHY row on a node DEAD past the repair grace doesn't.
     * Mutation G11: count raw HEALTHY rows.
     */
    @Test
    void aBadFileIsKeptUnlessTwoCountedHealthyReplicasRemain() {
        Upload u = commit("thin");
        String badNode = u.placed().get(0);
        String longDead = u.placed().get(2);
        markBad(u.chunkId(), badNode);
        dead(longDead, Duration.ofSeconds(31)); // past the 30 s repair grace
        wall.advance(GRACE.plusSeconds(1));

        gc.runPass();
        assertThat(nodes.deletes).as("only one HEALTHY replica counts").isEmpty();
        assertThat(taskNodes(u.chunkId())).isEmpty();

        backAlive(longDead);
        gc.runPass();
        assertThat(deletesOf(u.chunkId())).containsExactly(new FakeNodes.Delete(badNode, u.chunkId().toString(), false));
    }

    @Test
    void anAllBadChunkKeepsItsFiles() {
        Upload u = commit("all-bad");
        u.placed().forEach(n -> markBad(u.chunkId(), n));
        wall.advance(GRACE.plusSeconds(1));

        gc.runPass();

        assertThat(nodes.deletes).isEmpty();
        assertThat(rowsOf(u.chunkId())).isEqualTo(3);
    }

    /**
     * A copy in flight when the key is removed: repair records no replica row
     * for it (G6), and its target is a placement, so GC deletes the copy (G9).
     */
    @Test
    void repairNeverRecordsACopyForARetiredObjectAndGcFindsTheCopy() throws Exception {
        Upload u = commit("copying", 0, 1); // born with 2; position 2 is placed but holds nothing
        dead(u.placed().get(2), Duration.ZERO); // so the free node is the only target
        nodes.pushDelayMs = 500;
        Thread pass = Thread.ofVirtual().start(repair::runPass);
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (nodes.pushes.isEmpty() && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        assertThat(nodes.pushes).as("a copy started").hasSize(1);
        assertThat(nodes.pushes.getFirst().target()).isEqualTo(u.free());
        metadata.deleteObject("copying");
        pass.join(5000);

        assertThat(holds(u.free(), u.chunkId())).as("the copy landed").isTrue();
        assertThat(jdbc.sql("SELECT count(*) FROM chunk_replicas WHERE chunk_id = :c AND node_id = :n")
                .param("c", u.chunkId()).param("n", u.free()).query(Long.class).single())
                .as("no row for a copy of a removed object").isZero();

        wall.advance(GRACE.plusSeconds(1));
        gc.runPass();
        assertThat(holds(u.free(), u.chunkId())).as("GC deleted the copy through its placement").isFalse();
        assertThat(nodes.get(u.free()).tombstones).contains(u.chunkId().toString());
    }

    /** Mutation G14: a task kept after its node confirms, so the next pass sends it again. */
    @Test
    void aSecondPassIsANoOp() {
        commit("twice");
        commit("twice");
        gc.runPass();
        wall.advance(GRACE.plusSeconds(1));
        gc.runPass();
        int deletesAfterFirst = nodes.deletes.size();
        assertThat(deletesAfterFirst).isEqualTo(3);
        int calls = ALL.stream().mapToInt(n -> nodes.get(n).deleteCalls.get()).sum();

        GcService.PassResult second = gc.runPass();

        assertThat(second).isEqualTo(new GcService.PassResult(null, 0, 0, 0, 0, 0, 0, 0));
        assertThat(nodes.deletes).hasSize(deletesAfterFirst);
        assertThat(ALL.stream().mapToInt(n -> nodes.get(n).deleteCalls.get()).sum()).isEqualTo(calls);
    }

    /**
     * DeleteChunks calls carry at most gc-batch (4) IDs, at most one call per
     * node and gc-max-inflight (2) overall are in flight, and one pass still
     * finishes everything. Mutation G13: no overall bound.
     */
    @Test
    void deletesAreBatchedAndBounded() {
        UploadPlan plan = metadata.initiateUpload("big", 10 * CHUNK);
        List<CompletedChunk> done = new ArrayList<>();
        for (ChunkPlacement c : plan.chunks()) {
            for (Node n : c.nodes()) {
                nodes.get(n.nodeId()).held.put(c.chunkId().toString(),
                        new Held(ChunkStat.State.STATE_FOUND, CHUNK, sha(c.chunkId())));
            }
            done.add(new CompletedChunk(c.chunkId(), c.index(), CHUNK, sha(c.chunkId())));
        }
        metadata.completeUpload(plan.objectId(), "big", 10 * CHUNK, done);
        metadata.deleteObject("big");
        wall.advance(GRACE.plusSeconds(1));
        nodes.deleteDelayMs = 100;

        GcService.PassResult result = gc.runPass();

        assertThat(result.filesDeleted()).isEqualTo(30);
        assertThat(statusOf(plan.objectId())).isEqualTo("gone");
        assertThat(nodes.maxDeleteBatch()).isLessThanOrEqualTo(4);
        assertThat(nodes.maxDeleteCallsOnANode()).isEqualTo(1);
        assertThat(nodes.maxDeleteCallsInFlight()).isEqualTo(2);
    }

    /** rm and an overwrite of the same key serialize on the key row; the last to commit wins. */
    @Test
    void rmRacingAnOverwriteEndsInALastCommitWinsState() throws Exception {
        for (int round = 0; round < 10; round++) {
            String key = "contested-" + round;
            Upload old = commit(key);
            Upload next = initiate(key, 0, 1, 2);
            CountDownLatch go = new CountDownLatch(1);
            Thread put = Thread.ofVirtual().start(() -> {
                await(go);
                complete(next);
            });
            Thread rm = Thread.ofVirtual().start(() -> {
                await(go);
                metadata.deleteObject(key);
            });
            go.countDown();
            put.join(5000);
            rm.join(5000);

            String o = statusOf(old.objectId());
            String n = statusOf(next.objectId());
            if (metadata.getObject(key).isPresent()) {
                // rm went first: it removed the old version; the put then pointed the key at the new one.
                assertThat(metadata.getObject(key).orElseThrow().objectId()).isEqualTo(next.objectId());
                assertThat(List.of(o, n)).as("round %d", round).containsExactly("DELETED", "COMMITTED");
            } else {
                // The put went first; rm then removed the new version. The old one awaits supersede.
                assertThat(List.of(o, n)).as("round %d", round).containsExactly("COMMITTED", "DELETED");
            }
        }
    }

    @Test
    void lsAndStatShowCurrentKeysAndEveryReplica() {
        commit("ls/a");
        Upload b = commit("ls/b");
        commit("other");
        markBad(b.chunkId(), b.placed().get(0));

        assertThat(metadata.listObjects("ls/", "", 0).objects()).extracting(MetadataRepository.ObjectSummary::key)
                .containsExactly("ls/a", "ls/b");
        assertThat(metadata.listObjects("", "", 0).objects()).hasSize(3);

        MetadataRepository.ObjectDetail d = metadata.statObject("ls/b").orElseThrow();
        assertThat(d.objectId()).isEqualTo(b.objectId());
        assertThat(d.chunks().getFirst().replicas()).extracting(MetadataRepository.ReplicaDetail::state)
                .containsExactlyInAnyOrder("BAD", "HEALTHY", "HEALTHY");
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static byte[] sha(UUID id) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(id.toString().getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
