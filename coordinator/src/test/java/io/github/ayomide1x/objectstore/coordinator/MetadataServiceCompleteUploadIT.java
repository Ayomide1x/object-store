package io.github.ayomide1x.objectstore.coordinator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Stream;

import io.github.ayomide1x.objectstore.coordinator.FakeNodes.Held;
import io.github.ayomide1x.objectstore.coordinator.FakeNodes.Mode;
import io.github.ayomide1x.objectstore.coordinator.MetadataService.ChunkPlacement;
import io.github.ayomide1x.objectstore.coordinator.MetadataService.CompletedChunk;
import io.github.ayomide1x.objectstore.coordinator.MetadataService.UploadPlan;
import io.github.ayomide1x.objectstore.v1.ChunkStat;
import io.grpc.Status;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
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
 * Runs the upload lifecycle against a real Postgres with the Flyway schema
 * applied, because the guarantees under test (pointer swap, single-transaction
 * rollback, concurrent upserts, foreign keys) live in the database.
 *
 * <p>Four in-process fake nodes answer the coordinator's StatChunks calls.
 * {@link #holdOnPlacedNodes} makes every placed node report a chunk, as after
 * a fully successful upload; the replication tests change that per node.
 */
@Testcontainers
@SpringBootTest(properties = {
        "spring.grpc.server.port=0",
        "coordinator.node-rpc-timeout=" + MetadataServiceCompleteUploadIT.NODE_TIMEOUT_MS + "ms",
        "coordinator.liveness-sweep=0",
})
class MetadataServiceCompleteUploadIT {

    static final long NODE_TIMEOUT_MS = 500;

    @Container
    @ServiceConnection
    static PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:18");

    static final FakeNodes nodes = new FakeNodes(4);

    @DynamicPropertySource
    static void registerNodes(DynamicPropertyRegistry registry) {
        registry.add("coordinator.nodes", nodes::nodesProperty);
    }

    private static final long CHUNK = MetadataService.CHUNK_SIZE;

    private final MetadataService metadata;
    private final JdbcClient jdbc;
    private final LivenessService liveness;

    @Autowired
    MetadataServiceCompleteUploadIT(MetadataService metadata, JdbcClient jdbc, LivenessService liveness) {
        this.metadata = metadata;
        this.jdbc = jdbc;
        this.liveness = liveness;
    }

    @BeforeEach
    void clearObjects() {
        // nodes stays: NodeRegistry seeded it at context startup.
        jdbc.sql("TRUNCATE keys, chunk_replicas, chunk_placements, chunks, objects").update();
        nodes.reset();
        // Placement uses only ALIVE nodes; the sweeper is off, so they stay ALIVE.
        for (String n : List.of("node1", "node2", "node3", "node4")) {
            liveness.heartbeat(n, "test-instance");
        }
    }

    @AfterEach
    void dropFaultInjection() {
        jdbc.sql("DROP TRIGGER IF EXISTS fail_swap ON keys").update();
        jdbc.sql("DROP FUNCTION IF EXISTS fail_swap()").update();
        nodes.reset();
    }

    @Test
    void successfulCompleteRecordsThreeReplicasPerChunkAndPointsKeyAtObject() {
        long size = 2 * CHUNK + 5;
        UploadPlan plan = metadata.initiateUpload("photos/cat.jpg", size);

        assertThat(plan.chunks()).extracting(ChunkPlacement::sizeBytes).containsExactly(CHUNK, CHUNK, 5L);
        assertThat(statusOf(plan.objectId())).isEqualTo("PENDING");

        complete(plan, "photos/cat.jpg", size);

        assertThat(statusOf(plan.objectId())).isEqualTo("COMMITTED");
        for (ChunkPlacement c : plan.chunks()) {
            assertThat(replicasOf(c.chunkId())).as("chunk %d replicas", c.index())
                    .containsExactlyInAnyOrderElementsOf(nodeIds(c));
        }
        assertThat(pointer("photos/cat.jpg")).isEqualTo(plan.objectId());
        assertThat(resolvedChunkIds("photos/cat.jpg")).containsExactlyElementsOf(chunkIds(plan));
    }

    @Test
    void placementRotatesThreeOfFourNodes() {
        UploadPlan plan = metadata.initiateUpload("rotation", 8 * CHUNK);
        List<String> all = List.of("node1", "node2", "node3", "node4");
        Map<String, Integer> perNode = new java.util.HashMap<>();
        String previousLeftOut = null;
        for (ChunkPlacement c : plan.chunks()) {
            List<String> ids = nodeIds(c);
            assertThat(ids).as("chunk %d", c.index()).hasSize(3).doesNotHaveDuplicates();
            String leftOut = all.stream().filter(n -> !ids.contains(n)).findFirst().orElseThrow();
            if (previousLeftOut != null) {
                // Consecutive chunks leave out consecutive nodes.
                assertThat(leftOut).isEqualTo(all.get((all.indexOf(previousLeftOut) + 1) % 4));
            }
            previousLeftOut = leftOut;
            ids.forEach(n -> perNode.merge(n, 1, Integer::sum));
        }
        assertThat(perNode).containsOnlyKeys(all).allSatisfy((n, count) -> assertThat(count).isEqualTo(6));
    }

    @Test
    void zeroByteObjectResolvesWithEmptyChunkList() {
        UploadPlan plan = metadata.initiateUpload("empty", 0);
        assertThat(plan.chunks()).isEmpty();

        complete(plan, "empty", 0);

        StoredObject obj = metadata.getObject("empty").orElseThrow();
        assertThat(obj.objectId()).isEqualTo(plan.objectId());
        assertThat(obj.sizeBytes()).isZero();
        assertThat(obj.chunks()).isEmpty();
    }

    /** Mutation: GetObject resolves via objects.key, newest first, ignoring the pointer and status. */
    @Test
    void overwriteIsInvisibleUntilCommit() {
        UploadPlan v1 = metadata.initiateUpload("doc", CHUNK + 1);
        complete(v1, "doc", CHUNK + 1);

        UploadPlan v2 = metadata.initiateUpload("doc", 3 * CHUNK);
        assertThat(resolvedChunkIds("doc")).containsExactlyElementsOf(chunkIds(v1));

        complete(v2, "doc", 3 * CHUNK);
        assertThat(metadata.getObject("doc").orElseThrow().objectId()).isEqualTo(v2.objectId());
        assertThat(resolvedChunkIds("doc")).containsExactlyElementsOf(chunkIds(v2));

        // The superseded object is left intact for stage 8 GC.
        assertThat(statusOf(v1.objectId())).isEqualTo("COMMITTED");
        assertThat(count("SELECT count(*) FROM chunks WHERE object_id = :id", v1.objectId())).isEqualTo(2);
    }

    /**
     * Proves the commit phase is atomic. A test-only trigger makes the key
     * swap, the last statement, fail after the replica inserts and status
     * update have run. Mutation: run the commit phase outside its transaction.
     */
    @Test
    void failedOverwriteLeavesOldObjectIntact() {
        UploadPlan v1 = metadata.initiateUpload("boom", 10);
        complete(v1, "boom", 10);

        jdbc.sql("""
                CREATE FUNCTION fail_swap() RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN
                    IF NEW.key = 'boom' THEN RAISE EXCEPTION 'injected failure on key swap'; END IF;
                    RETURN NEW;
                END $$
                """).update();
        jdbc.sql("CREATE TRIGGER fail_swap BEFORE INSERT OR UPDATE ON keys FOR EACH ROW EXECUTE FUNCTION fail_swap()")
                .update();

        UploadPlan v2 = metadata.initiateUpload("boom", CHUNK + 7);
        assertThatThrownBy(() -> complete(v2, "boom", CHUNK + 7)).hasMessageContaining("injected failure");

        assertThat(pointer("boom")).isEqualTo(v1.objectId());
        assertThat(resolvedChunkIds("boom")).containsExactlyElementsOf(chunkIds(v1));
        assertThat(statusOf(v2.objectId())).isEqualTo("PENDING");
        assertThat(replicaCount(v2.objectId())).isZero();
    }

    /** Mutation: replace the upsert in swapKey with read-then-write (SELECT, then UPDATE or INSERT). */
    @Test
    void concurrentOverwritesAllSucceedAndLeaveOnePointer() throws Exception {
        int rounds = 20;
        int writers = 8; // below Hikari's default pool of 10, so every writer holds a connection
        try (ExecutorService pool = Executors.newFixedThreadPool(writers)) {
            for (int round = 0; round < rounds; round++) {
                // Even rounds race to create a new key; odd rounds race to replace an existing one.
                String key = "race-" + round;
                if (round % 2 == 1) {
                    complete(metadata.initiateUpload(key, 10), key, 10);
                }
                List<UploadPlan> plans = new ArrayList<>();
                for (int i = 0; i < writers; i++) {
                    UploadPlan plan = metadata.initiateUpload(key, 10);
                    holdOnPlacedNodes(plan);
                    plans.add(plan);
                }

                CountDownLatch start = new CountDownLatch(1);
                List<Future<?>> futures = new ArrayList<>();
                for (UploadPlan plan : plans) {
                    futures.add(pool.submit(() -> {
                        start.await();
                        send(new Request(plan.objectId(), key, 10, completedChunks(plan)));
                        return null;
                    }));
                }
                start.countDown();
                for (Future<?> f : futures) {
                    try {
                        f.get();
                    } catch (ExecutionException e) {
                        throw new AssertionError("round %d: a concurrent complete failed".formatted(round), e.getCause());
                    }
                }

                assertThat(jdbc.sql("SELECT count(*) FROM keys WHERE key = :key").param("key", key)
                        .query(Long.class).single()).as("round %d keys rows", round).isEqualTo(1);
                UUID winner = pointer(key);
                assertThat(plans).extracting(UploadPlan::objectId).as("round %d winner", round).contains(winner);
                for (UploadPlan plan : plans) {
                    assertThat(statusOf(plan.objectId())).as("round %d status", round).isEqualTo("COMMITTED");
                }
                UploadPlan winnerPlan = plans.stream().filter(p -> p.objectId().equals(winner)).findFirst().orElseThrow();
                assertThat(resolvedChunkIds(key)).as("round %d chunks", round)
                        .containsExactlyElementsOf(chunkIds(winnerPlan));
            }
        }
    }

    /** One placed node's report for a chunk, relative to what the client sent. */
    enum Report { MATCH, MISSING, OTHER_HASH, WRONG_SIZE, BAD_HEADER, UNREACHABLE }

    static Stream<Arguments> replicaVerificationCases() {
        return Stream.of(
                Arguments.of("all three hold it", List.of(Report.MATCH, Report.MATCH, Report.MATCH), 3),
                Arguments.of("one missing", List.of(Report.MATCH, Report.MISSING, Report.MATCH), 2),
                Arguments.of("one with another hash", List.of(Report.OTHER_HASH, Report.MATCH, Report.MATCH), 2),
                Arguments.of("one with the wrong size", List.of(Report.MATCH, Report.MATCH, Report.WRONG_SIZE), 2),
                Arguments.of("one with a bad header", List.of(Report.MATCH, Report.BAD_HEADER, Report.MATCH), 2),
                Arguments.of("one unreachable", List.of(Report.UNREACHABLE, Report.MATCH, Report.MATCH), 2),
                Arguments.of("two missing", List.of(Report.MATCH, Report.MISSING, Report.MISSING), 0),
                Arguments.of("only one matches", List.of(Report.OTHER_HASH, Report.MATCH, Report.BAD_HEADER), 0),
                Arguments.of("client hash matches no node",
                        List.of(Report.OTHER_HASH, Report.OTHER_HASH, Report.OTHER_HASH), 0));
    }

    /**
     * A replica counts only if its node reports a valid header with the
     * planned size and the client's SHA-256. Only counted replicas are
     * recorded; fewer than 2 refuses the commit. 0 in wantRecorded means
     * refused. Mutations: record every placed node; skip the hash comparison;
     * quorum of 1.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("replicaVerificationCases")
    void onlyVerifiedReplicasCount(String name, List<Report> reports, int wantRecorded) {
        UploadPlan plan = metadata.initiateUpload("verify", 10);
        ChunkPlacement chunk = plan.chunks().getFirst();
        byte[] sha = fakeSha(chunk.chunkId());
        List<String> placed = nodeIds(chunk);
        List<String> wantNodes = new ArrayList<>();
        for (int i = 0; i < placed.size(); i++) {
            FakeNodes.FakeNode node = nodes.get(placed.get(i));
            String id = chunk.chunkId().toString();
            switch (reports.get(i)) {
                case MATCH -> {
                    node.held.put(id, new Held(ChunkStat.State.STATE_FOUND, 10, sha));
                    wantNodes.add(node.nodeId);
                }
                case MISSING -> {
                }
                case OTHER_HASH -> node.held.put(id, new Held(ChunkStat.State.STATE_FOUND, 10, new byte[32]));
                case WRONG_SIZE -> node.held.put(id, new Held(ChunkStat.State.STATE_FOUND, 11, sha));
                case BAD_HEADER -> node.held.put(id, new Held(ChunkStat.State.STATE_BAD_HEADER, 0, null));
                case UNREACHABLE -> node.mode = Mode.FAIL;
            }
        }
        Request req = new Request(plan.objectId(), "verify", 10, completedChunks(plan));

        if (wantRecorded == 0) {
            assertThatThrownBy(() -> send(req))
                    .isInstanceOfSatisfying(RequestException.class,
                            e -> assertThat(e.code()).isEqualTo(Status.Code.FAILED_PRECONDITION))
                    .hasMessageContaining("verified replicas, need 2");
            assertThat(statusOf(plan.objectId())).isEqualTo("PENDING");
            assertThat(replicaCount(plan.objectId())).isZero();
            assertThat(metadata.getObject("verify")).isEmpty();
            return;
        }
        send(req);
        assertThat(replicasOf(chunk.chunkId())).containsExactlyInAnyOrderElementsOf(wantNodes).hasSize(wantRecorded);
        assertThat(jdbc.sql("SELECT sha256 FROM chunks WHERE chunk_id = :id").param("id", chunk.chunkId())
                .query((rs, i) -> rs.getBytes(1)).single()).as("recorded hash is the agreed value").isEqualTo(sha);
    }

    /** One short chunk is enough to refuse the whole object, and nothing is written. */
    @Test
    void commitRefusedWhenAnyChunkHasFewerThanTwoVerifiedReplicas() {
        UploadPlan plan = metadata.initiateUpload("short", 3 * CHUNK);
        holdOnPlacedNodes(plan);
        ChunkPlacement middle = plan.chunks().get(1);
        for (String nodeId : nodeIds(middle).subList(1, 3)) {
            nodes.get(nodeId).held.remove(middle.chunkId().toString());
        }

        assertThatThrownBy(() -> send(new Request(plan.objectId(), "short", 3 * CHUNK, completedChunks(plan))))
                .isInstanceOfSatisfying(RequestException.class,
                        e -> assertThat(e.code()).isEqualTo(Status.Code.FAILED_PRECONDITION))
                .hasMessageContaining("chunk " + middle.chunkId() + " (index 1) has 1 verified replicas, need 2");

        assertThat(statusOf(plan.objectId())).isEqualTo("PENDING");
        assertThat(replicaCount(plan.objectId())).isZero();
        assertThat(jdbc.sql("SELECT count(*) FROM chunks WHERE object_id = :id AND sha256 IS NOT NULL")
                .param("id", plan.objectId()).query(Long.class).single()).isZero();
        assertThat(metadata.getObject("short")).isEmpty();
    }

    /**
     * A hung node costs one deadline for the whole object, however many chunks
     * it holds, because each node gets one batched call. Mutation: no deadline
     * on the call (the @Timeout turns the hang into a failure).
     */
    @Test
    @Timeout(value = 20, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void hungNodeCostsOneDeadlineWhateverTheObjectSize() {
        long size = 40 * CHUNK; // 40 chunks; the hung node is placed on 30 of them
        UploadPlan plan = metadata.initiateUpload("hung", size);
        holdOnPlacedNodes(plan);
        nodes.get("node3").mode = Mode.HANG;

        long start = System.nanoTime();
        send(new Request(plan.objectId(), "hung", size, completedChunks(plan)));
        Duration elapsed = Duration.ofNanos(System.nanoTime() - start);

        assertThat(elapsed).as("one %d ms deadline plus a margin", NODE_TIMEOUT_MS)
                .isLessThan(Duration.ofMillis(NODE_TIMEOUT_MS + 1500));
        assertThat(statusOf(plan.objectId())).isEqualTo("COMMITTED");
        for (ChunkPlacement c : plan.chunks()) {
            List<String> want = nodeIds(c).stream().filter(n -> !n.equals("node3")).toList();
            assertThat(replicasOf(c.chunkId())).as("chunk %d", c.index()).containsExactlyInAnyOrderElementsOf(want);
        }
    }

    /**
     * A response that identifies as another node records nothing for the node
     * that was called, even though it reports holding the chunk. Mutation:
     * accept a response from the wrong node.
     */
    @Test
    void responseFromTheWrongNodeRecordsNothingForIt() {
        UploadPlan plan = metadata.initiateUpload("identity", 10);
        holdOnPlacedNodes(plan);
        ChunkPlacement chunk = plan.chunks().getFirst();
        List<String> placed = nodeIds(chunk);
        // The call meant for the third placed node is answered by a node claiming
        // to be the first, as when a stale address reaches the wrong container.
        String misdirected = placed.get(2);
        nodes.get(misdirected).answerAs = placed.get(0);

        send(new Request(plan.objectId(), "identity", 10, completedChunks(plan)));

        assertThat(replicasOf(chunk.chunkId()))
                .as("nothing recorded for %s", misdirected)
                .containsExactlyInAnyOrderElementsOf(placed.subList(0, 2));
    }

    /**
     * UNAVAILABLE once (a stale address or dead connection) is retried on a
     * fresh channel within the same deadline, and the replica counts.
     * Mutation: no retry on UNAVAILABLE.
     */
    @Test
    void unavailableOnceIsRetriedAndTheReplicaCounts() {
        UploadPlan plan = metadata.initiateUpload("retry", 10);
        holdOnPlacedNodes(plan);
        ChunkPlacement chunk = plan.chunks().getFirst();
        FakeNodes.FakeNode flaky = nodes.get(nodeIds(chunk).get(1));
        flaky.mode = Mode.FAIL_ONCE;

        send(new Request(plan.objectId(), "retry", 10, completedChunks(plan)));

        assertThat(flaky.calls.get()).as("calls to %s", flaky.nodeId).isEqualTo(2);
        assertThat(replicasOf(chunk.chunkId())).containsExactlyInAnyOrderElementsOf(nodeIds(chunk));
    }

    /** What a test case does to a valid request before sending it. */
    private record Request(UUID objectId, String key, long sizeBytes, List<CompletedChunk> chunks) {
    }

    /** Inputs a test case can build its request from. */
    private record Fixture(UploadPlan plan, String key, long size, UploadPlan foreign) {

        Request valid() {
            return new Request(plan.objectId(), key, size, completedChunks(plan));
        }

        Request withChunks(List<CompletedChunk> chunks) {
            return new Request(plan.objectId(), key, size, chunks);
        }

        CompletedChunk chunk(int i) {
            return completedChunks(plan).get(i);
        }
    }

    static Stream<Arguments> validationCases() {
        Function<Fixture, Request> valid = Fixture::valid;
        return Stream.of(
                Arguments.of("valid request", valid, Status.Code.OK, null),
                Arguments.of("unknown object", (Function<Fixture, Request>) f ->
                        new Request(UUID.randomUUID(), f.key(), f.size(), completedChunks(f.plan())),
                        Status.Code.NOT_FOUND, "not found"),
                Arguments.of("key mismatch", (Function<Fixture, Request>) f ->
                        new Request(f.plan().objectId(), "other", f.size(), completedChunks(f.plan())),
                        Status.Code.INVALID_ARGUMENT, "does not match upload key"),
                Arguments.of("empty key", (Function<Fixture, Request>) f ->
                        new Request(f.plan().objectId(), "", f.size(), completedChunks(f.plan())),
                        Status.Code.INVALID_ARGUMENT, "does not match upload key"),
                Arguments.of("size mismatch", (Function<Fixture, Request>) f ->
                        new Request(f.plan().objectId(), f.key(), f.size() + 1, completedChunks(f.plan())),
                        Status.Code.INVALID_ARGUMENT, "does not match upload size"),
                Arguments.of("missing chunk", (Function<Fixture, Request>) f ->
                        f.withChunks(List.of(f.chunk(0))),
                        Status.Code.INVALID_ARGUMENT, "has 2 chunks, request has 1"),
                Arguments.of("extra chunk from another object", (Function<Fixture, Request>) f ->
                        f.withChunks(List.of(f.chunk(0), f.chunk(1), completedChunks(f.foreign()).getFirst())),
                        Status.Code.INVALID_ARGUMENT, "has 2 chunks, request has 3"),
                Arguments.of("chunk from another object replaces one", (Function<Fixture, Request>) f ->
                        f.withChunks(List.of(f.chunk(0), completedChunks(f.foreign()).getFirst())),
                        Status.Code.INVALID_ARGUMENT, "was not allocated to object"),
                Arguments.of("chunk listed twice", (Function<Fixture, Request>) f ->
                        f.withChunks(List.of(f.chunk(0), f.chunk(0))),
                        Status.Code.INVALID_ARGUMENT, "listed twice"),
                Arguments.of("indexes swapped", (Function<Fixture, Request>) f ->
                        f.withChunks(List.of(withIndex(f.chunk(0), 1), withIndex(f.chunk(1), 0))),
                        Status.Code.INVALID_ARGUMENT, "has index 1, allocated 0"),
                Arguments.of("index gap", (Function<Fixture, Request>) f ->
                        f.withChunks(List.of(f.chunk(0), withIndex(f.chunk(1), 2))),
                        Status.Code.INVALID_ARGUMENT, "has index 2, allocated 1"),
                Arguments.of("wrong chunk size", (Function<Fixture, Request>) f ->
                        f.withChunks(List.of(f.chunk(0), withSize(f.chunk(1), f.chunk(1).sizeBytes() - 1))),
                        Status.Code.INVALID_ARGUMENT, "has size 9, allocated 10"),
                Arguments.of("sha256 missing", (Function<Fixture, Request>) f ->
                        f.withChunks(List.of(f.chunk(0), withSha(f.chunk(1), new byte[0]))),
                        Status.Code.INVALID_ARGUMENT, "sha256 is 0 bytes, want 32"),
                Arguments.of("sha256 31 bytes", (Function<Fixture, Request>) f ->
                        f.withChunks(List.of(f.chunk(0), withSha(f.chunk(1), new byte[31]))),
                        Status.Code.INVALID_ARGUMENT, "sha256 is 31 bytes, want 32"),
                Arguments.of("sha256 33 bytes", (Function<Fixture, Request>) f ->
                        f.withChunks(List.of(f.chunk(0), withSha(f.chunk(1), new byte[33]))),
                        Status.Code.INVALID_ARGUMENT, "sha256 is 33 bytes, want 32"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("validationCases")
    void completeUploadValidatesAgainstIssuedPlan(String name, Function<Fixture, Request> build,
            Status.Code want, String messagePart) {
        long size = CHUNK + 10; // two chunks
        Fixture f = new Fixture(metadata.initiateUpload("v", size), "v", size, metadata.initiateUpload("other", 10));
        holdOnPlacedNodes(f.plan());
        holdOnPlacedNodes(f.foreign());
        Request req = build.apply(f);

        if (want == Status.Code.OK) {
            send(req);
            assertThat(statusOf(f.plan().objectId())).isEqualTo("COMMITTED");
            assertThat(pointer("v")).isEqualTo(f.plan().objectId());
            return;
        }
        // The message fragment pins each case to the check it targets, not just any rejection.
        assertThatThrownBy(() -> send(req))
                .isInstanceOfSatisfying(RequestException.class, e -> assertThat(e.code()).isEqualTo(want))
                .hasMessageContaining(messagePart);

        // A rejected complete writes nothing: still PENDING, no replicas, key unresolved.
        assertThat(statusOf(f.plan().objectId())).isEqualTo("PENDING");
        assertThat(replicaCount(f.plan().objectId())).isZero();
        assertThat(metadata.getObject("v")).isEmpty();
    }

    /** Mutation: completeUpload does not record the chunk hashes. */
    @Test
    void checksumsAreRecordedAndReturnedByGetObject() {
        long size = 2 * CHUNK + 3;
        UploadPlan plan = metadata.initiateUpload("hashed", size);
        assertThat(jdbc.sql("SELECT count(*) FROM chunks WHERE object_id = :id AND sha256 IS NULL")
                .param("id", plan.objectId()).query(Long.class).single())
                .as("hashes are unknown while PENDING").isEqualTo(3);

        complete(plan, "hashed", size);

        List<byte[]> want = completedChunks(plan).stream().map(CompletedChunk::sha256).toList();
        assertThat(jdbc.sql("SELECT sha256 FROM chunks WHERE object_id = :id ORDER BY chunk_index")
                .param("id", plan.objectId()).query((rs, i) -> rs.getBytes(1)).list())
                .containsExactlyElementsOf(want);
        assertThat(metadata.getObject("hashed").orElseThrow().chunks())
                .extracting(StoredObject.Chunk::sha256)
                .containsExactlyElementsOf(want);
    }

    @Test
    void completingCommittedObjectAgainIsNoOp() {
        UploadPlan plan = metadata.initiateUpload("again", CHUNK + 10);
        complete(plan, "again", CHUNK + 10);
        UploadPlan newer = metadata.initiateUpload("again", 10);
        complete(newer, "again", 10);

        // Retrying the first complete must not move the pointer back or add rows.
        complete(plan, "again", CHUNK + 10);

        assertThat(pointer("again")).isEqualTo(newer.objectId());
        assertThat(replicaCount(plan.objectId())).isEqualTo(6);
    }

    /** Completes plan as after a fully successful upload: every placed node holds every chunk. */
    private void complete(UploadPlan plan, String key, long size) {
        holdOnPlacedNodes(plan);
        metadata.completeUpload(plan.objectId(), key, size, completedChunks(plan));
    }

    private void send(Request r) {
        metadata.completeUpload(r.objectId(), r.key(), r.sizeBytes(), r.chunks());
    }

    /** Makes every placed node report every chunk of plan with its planned size and the client's hash. */
    private static void holdOnPlacedNodes(UploadPlan plan) {
        for (ChunkPlacement c : plan.chunks()) {
            for (String nodeId : nodeIds(c)) {
                nodes.get(nodeId).held.put(c.chunkId().toString(),
                        new Held(ChunkStat.State.STATE_FOUND, c.sizeBytes(), fakeSha(c.chunkId())));
            }
        }
    }

    /**
     * The completion a well-behaved client sends, with a SHA-256 per chunk.
     * The coordinator never sees bytes, so any 32 bytes stand in for the hash;
     * this one is derived from the chunk ID so each chunk's differs.
     */
    private static List<CompletedChunk> completedChunks(UploadPlan plan) {
        return plan.chunks().stream()
                .map(c -> new CompletedChunk(c.chunkId(), c.index(), c.sizeBytes(), fakeSha(c.chunkId())))
                .toList();
    }

    private static byte[] fakeSha(UUID chunkId) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(chunkId.toString().getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private static CompletedChunk withIndex(CompletedChunk c, int index) {
        return new CompletedChunk(c.chunkId(), index, c.sizeBytes(), c.sha256());
    }

    private static CompletedChunk withSize(CompletedChunk c, long size) {
        return new CompletedChunk(c.chunkId(), c.index(), size, c.sha256());
    }

    private static CompletedChunk withSha(CompletedChunk c, byte[] sha256) {
        return new CompletedChunk(c.chunkId(), c.index(), c.sizeBytes(), sha256);
    }

    private static List<String> nodeIds(ChunkPlacement c) {
        return c.nodes().stream().map(Node::nodeId).toList();
    }

    private static List<UUID> chunkIds(UploadPlan plan) {
        return plan.chunks().stream().map(ChunkPlacement::chunkId).toList();
    }

    /** Chunk IDs the read path returns for key, in order. Checks they all belong to one object. */
    private List<UUID> resolvedChunkIds(String key) {
        StoredObject obj = metadata.getObject(key).orElseThrow();
        List<UUID> ids = obj.chunks().stream().map(StoredObject.Chunk::chunkId).toList();
        Set<UUID> owners = new HashSet<>(jdbc.sql("SELECT object_id FROM chunks WHERE chunk_id IN (:ids)")
                .param("ids", ids).query(UUID.class).list());
        assertThat(owners).as("owners of chunks resolved for %s", key).containsExactly(obj.objectId());
        return ids;
    }

    private List<String> replicasOf(UUID chunkId) {
        return jdbc.sql("SELECT node_id FROM chunk_replicas WHERE chunk_id = :id ORDER BY node_id")
                .param("id", chunkId).query(String.class).list();
    }

    private long replicaCount(UUID objectId) {
        return count("SELECT count(*) FROM chunk_replicas r JOIN chunks c USING (chunk_id) WHERE c.object_id = :id",
                objectId);
    }

    private UUID pointer(String key) {
        return jdbc.sql("SELECT object_id FROM keys WHERE key = :key").param("key", key).query(UUID.class).single();
    }

    private String statusOf(UUID objectId) {
        return jdbc.sql("SELECT status FROM objects WHERE object_id = :id").param("id", objectId)
                .query(String.class).single();
    }

    private long count(String sql, UUID id) {
        return jdbc.sql(sql).param("id", id).query(Long.class).single();
    }
}
