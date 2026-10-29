package io.github.ayomide1x.objectstore.coordinator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Stream;

import io.github.ayomide1x.objectstore.coordinator.MetadataService.CompletedChunk;
import io.github.ayomide1x.objectstore.coordinator.MetadataService.UploadPlan;
import io.grpc.Status;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Runs the upload lifecycle against a real Postgres with the Flyway schema
 * applied, because the guarantees under test (pointer swap, single-transaction
 * rollback, concurrent upserts, foreign keys) live in the database.
 *
 * <p>Two nodes are registered; placement always picks node1, so node2 is a
 * real node that is never a placement target.
 */
@Testcontainers
@SpringBootTest(properties = {
        "spring.grpc.server.port=0",
        "coordinator.nodes=node1|node1:9101|127.0.0.1:9101,node2|node2:9102|127.0.0.1:9102",
})
class MetadataServiceCompleteUploadIT {

    @Container
    @ServiceConnection
    static PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:18");

    private static final long CHUNK = MetadataService.CHUNK_SIZE;

    private final MetadataService metadata;
    private final JdbcClient jdbc;

    @Autowired
    MetadataServiceCompleteUploadIT(MetadataService metadata, JdbcClient jdbc) {
        this.metadata = metadata;
        this.jdbc = jdbc;
    }

    @BeforeEach
    void clearObjects() {
        // nodes stays: NodeRegistry seeded it at context startup.
        jdbc.sql("TRUNCATE keys, chunk_replicas, chunk_placements, chunks, objects").update();
    }

    @AfterEach
    void dropFaultInjection() {
        jdbc.sql("DROP TRIGGER IF EXISTS fail_swap ON keys").update();
        jdbc.sql("DROP FUNCTION IF EXISTS fail_swap()").update();
    }

    @Test
    void successfulCompleteWritesRowsAndPointsKeyAtObject() {
        long size = 2 * CHUNK + 5;
        UploadPlan plan = metadata.initiateUpload("photos/cat.jpg", size);

        assertThat(plan.chunks()).extracting(MetadataService.ChunkPlacement::sizeBytes)
                .containsExactly(CHUNK, CHUNK, 5L);
        assertThat(statusOf(plan.objectId())).isEqualTo("PENDING");

        complete(plan, "photos/cat.jpg", size);

        assertThat(statusOf(plan.objectId())).isEqualTo("COMMITTED");
        assertThat(jdbc.sql("SELECT chunk_index || ':' || size_bytes FROM chunks WHERE object_id = :id ORDER BY 1")
                .param("id", plan.objectId()).query(String.class).list())
                .containsExactly("0:" + CHUNK, "1:" + CHUNK, "2:5");
        assertThat(count("""
                SELECT count(*) FROM chunk_replicas r JOIN chunks c USING (chunk_id)
                WHERE c.object_id = :id AND r.node_id = 'node1'
                """, plan.objectId())).isEqualTo(3);
        assertThat(pointer("photos/cat.jpg")).isEqualTo(plan.objectId());
        assertThat(resolvedChunkIds("photos/cat.jpg")).containsExactlyElementsOf(chunkIds(plan));
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
     * Proves completeUpload is atomic. A test-only trigger makes the key swap,
     * the last statement, fail after the replica inserts and status update have
     * run. Mutation: remove @Transactional from completeUpload.
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
        assertThat(count("""
                SELECT count(*) FROM chunk_replicas r JOIN chunks c USING (chunk_id) WHERE c.object_id = :id
                """, v2.objectId())).isZero();
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
                    plans.add(metadata.initiateUpload(key, 10));
                }

                CountDownLatch start = new CountDownLatch(1);
                List<Future<?>> futures = new ArrayList<>();
                for (UploadPlan plan : plans) {
                    futures.add(pool.submit(() -> {
                        start.await();
                        complete(plan, key, 10);
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
                Arguments.of("no acknowledgements", (Function<Fixture, Request>) f ->
                        f.withChunks(List.of(f.chunk(0), withAcks(f.chunk(1), List.of()))),
                        Status.Code.INVALID_ARGUMENT, "no acknowledged replicas"),
                Arguments.of("same node acknowledged twice", (Function<Fixture, Request>) f ->
                        f.withChunks(List.of(f.chunk(0), withAcks(f.chunk(1), List.of("node1", "node1")))),
                        Status.Code.INVALID_ARGUMENT, "lists a node twice"),
                Arguments.of("ack from registered node outside placement", (Function<Fixture, Request>) f ->
                        f.withChunks(List.of(f.chunk(0), withAcks(f.chunk(1), List.of("node1", "node2")))),
                        Status.Code.INVALID_ARGUMENT, "node node2 was not a placement target"),
                Arguments.of("ack from unknown node", (Function<Fixture, Request>) f ->
                        f.withChunks(List.of(f.chunk(0), withAcks(f.chunk(1), List.of("node9")))),
                        Status.Code.INVALID_ARGUMENT, "node node9 was not a placement target"),
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
        assertThat(count("""
                SELECT count(*) FROM chunk_replicas r JOIN chunks c USING (chunk_id) WHERE c.object_id = :id
                """, f.plan().objectId())).isZero();
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
        assertThat(count("""
                SELECT count(*) FROM chunk_replicas r JOIN chunks c USING (chunk_id) WHERE c.object_id = :id
                """, plan.objectId())).isEqualTo(2);
    }

    private void complete(UploadPlan plan, String key, long size) {
        metadata.completeUpload(plan.objectId(), key, size, completedChunks(plan));
    }

    private void send(Request r) {
        metadata.completeUpload(r.objectId(), r.key(), r.sizeBytes(), r.chunks());
    }

    /**
     * The completion a well-behaved client sends: every chunk acknowledged by
     * every placed node, with a SHA-256. The coordinator never sees bytes, so
     * any 32 bytes stand in for the hash; this one is derived from the chunk
     * ID so each chunk's differs.
     */
    private static List<CompletedChunk> completedChunks(UploadPlan plan) {
        return plan.chunks().stream()
                .map(c -> new CompletedChunk(c.chunkId(), c.index(), c.sizeBytes(),
                        c.nodes().stream().map(Node::nodeId).toList(), fakeSha(c.chunkId())))
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
        return new CompletedChunk(c.chunkId(), index, c.sizeBytes(), c.ackedNodeIds(), c.sha256());
    }

    private static CompletedChunk withSize(CompletedChunk c, long size) {
        return new CompletedChunk(c.chunkId(), c.index(), size, c.ackedNodeIds(), c.sha256());
    }

    private static CompletedChunk withAcks(CompletedChunk c, List<String> acks) {
        return new CompletedChunk(c.chunkId(), c.index(), c.sizeBytes(), acks, c.sha256());
    }

    private static CompletedChunk withSha(CompletedChunk c, byte[] sha256) {
        return new CompletedChunk(c.chunkId(), c.index(), c.sizeBytes(), c.ackedNodeIds(), sha256);
    }

    private static List<UUID> chunkIds(UploadPlan plan) {
        return plan.chunks().stream().map(MetadataService.ChunkPlacement::chunkId).toList();
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
