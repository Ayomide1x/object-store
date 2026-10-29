package io.github.ayomide1x.objectstore.coordinator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import io.grpc.Status;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Runs MetadataService.completeUpload against a real Postgres with the Flyway
 * schema applied, because the guarantees under test (unique constraint,
 * single-transaction rollback, concurrent inserts) live in the database.
 */
@Testcontainers
@SpringBootTest(properties = {
        "spring.grpc.server.port=0",
        "coordinator.nodes=node1|node1:9101|127.0.0.1:9101",
})
class MetadataServiceCompleteUploadIT {

    @Container
    @ServiceConnection
    static PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:18");

    private static final long SIZE = 1234;

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
        jdbc.sql("TRUNCATE chunk_replicas, chunks, objects").update();
    }

    @Test
    void successfulCompleteWritesObjectChunkAndReplicaRows() {
        UUID objectId = UUID.randomUUID();
        UUID chunkId = UUID.randomUUID();

        complete(objectId, chunkId, "photos/cat.jpg");

        assertThat(jdbc.sql("SELECT key, size_bytes FROM objects WHERE object_id = :id")
                .param("id", objectId)
                .query((rs, i) -> rs.getString("key") + ":" + rs.getLong("size_bytes"))
                .list())
                .containsExactly("photos/cat.jpg:" + SIZE);
        assertThat(jdbc.sql("SELECT object_id, chunk_index, size_bytes FROM chunks WHERE chunk_id = :id")
                .param("id", chunkId)
                .query((rs, i) -> rs.getObject("object_id", UUID.class) + ":" + rs.getInt("chunk_index") + ":"
                        + rs.getLong("size_bytes"))
                .list())
                .containsExactly(objectId + ":0:" + SIZE);
        assertThat(jdbc.sql("SELECT node_id FROM chunk_replicas WHERE chunk_id = :id")
                .param("id", chunkId)
                .query(String.class)
                .list())
                .containsExactly("node1");
    }

    @Test
    void secondCompleteForSameKeyIsAlreadyExistsAndLeavesNoPartialRows() {
        UUID firstObject = UUID.randomUUID();
        UUID firstChunk = UUID.randomUUID();
        complete(firstObject, firstChunk, "dup");

        UUID secondObject = UUID.randomUUID();
        UUID secondChunk = UUID.randomUUID();
        assertThatThrownBy(() -> complete(secondObject, secondChunk, "dup"))
                .isInstanceOfSatisfying(RequestException.class,
                        e -> assertThat(e.code()).isEqualTo(Status.Code.ALREADY_EXISTS));

        assertRowCounts(1, 1, 1);
        assertThat(jdbc.sql("SELECT object_id FROM objects WHERE key = 'dup'").query(UUID.class).single())
                .isEqualTo(firstObject);
        assertThat(count("SELECT count(*) FROM objects WHERE object_id = :id", secondObject)).isZero();
        assertThat(count("SELECT count(*) FROM chunks WHERE chunk_id = :id", secondChunk)).isZero();
        assertThat(count("SELECT count(*) FROM chunk_replicas WHERE chunk_id = :id", secondChunk)).isZero();
    }

    @Test
    void concurrentCompletesForOneKeyProduceExactlyOneSuccess() throws Exception {
        int attempts = 8; // below Hikari's default pool of 10, so every attempt holds a connection
        CountDownLatch start = new CountDownLatch(1);
        List<UUID> objectIds = new ArrayList<>();
        List<Future<?>> futures = new ArrayList<>();

        try (ExecutorService pool = Executors.newFixedThreadPool(attempts)) {
            for (int i = 0; i < attempts; i++) {
                UUID objectId = UUID.randomUUID();
                objectIds.add(objectId);
                futures.add(pool.submit(() -> {
                    start.await();
                    complete(objectId, UUID.randomUUID(), "race");
                    return null;
                }));
            }
            start.countDown();

            int successes = 0;
            for (Future<?> f : futures) {
                try {
                    f.get();
                    successes++;
                } catch (java.util.concurrent.ExecutionException e) {
                    assertThat(e.getCause())
                            .isInstanceOfSatisfying(RequestException.class,
                                    re -> assertThat(re.code()).isEqualTo(Status.Code.ALREADY_EXISTS));
                }
            }
            assertThat(successes).isEqualTo(1);
        }

        assertRowCounts(1, 1, 1);
        UUID winner = jdbc.sql("SELECT object_id FROM objects WHERE key = 'race'").query(UUID.class).single();
        assertThat(objectIds).contains(winner);
        assertThat(count("SELECT count(*) FROM chunks WHERE object_id = :id", winner)).isEqualTo(1);
    }

    /**
     * The only test here that proves completeUpload is atomic. The objects and
     * chunks inserts succeed, then the chunk_replicas insert violates the
     * foreign key to nodes. Without a single transaction the first two rows
     * would survive. The duplicate-key tests fail on the first insert, so they
     * cannot tell.
     */
    @Test
    void failureOnLastInsertRollsBackEarlierInserts() {
        UUID objectId = UUID.randomUUID();
        UUID chunkId = UUID.randomUUID();

        assertThatThrownBy(() -> metadata.completeUpload(objectId, "orphan", SIZE,
                List.of(new MetadataService.CompletedChunk(chunkId, 0, SIZE, List.of("no-such-node")))))
                .isInstanceOfSatisfying(RequestException.class,
                        e -> assertThat(e.code()).isEqualTo(Status.Code.INVALID_ARGUMENT));

        assertRowCounts(0, 0, 0);
    }

    private void complete(UUID objectId, UUID chunkId, String key) {
        metadata.completeUpload(objectId, key, SIZE,
                List.of(new MetadataService.CompletedChunk(chunkId, 0, SIZE, List.of("node1"))));
    }

    private void assertRowCounts(long objects, long chunks, long replicas) {
        assertThat(jdbc.sql("SELECT count(*) FROM objects").query(Long.class).single()).isEqualTo(objects);
        assertThat(jdbc.sql("SELECT count(*) FROM chunks").query(Long.class).single()).isEqualTo(chunks);
        assertThat(jdbc.sql("SELECT count(*) FROM chunk_replicas").query(Long.class).single()).isEqualTo(replicas);
    }

    private long count(String sql, UUID id) {
        return jdbc.sql(sql).param("id", id).query(Long.class).single();
    }
}
