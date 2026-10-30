package io.github.ayomide1x.objectstore.coordinator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import io.github.ayomide1x.objectstore.coordinator.LivenessTracker.State;
import io.github.ayomide1x.objectstore.coordinator.LivenessTrackerTest.FakeTicker;
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
 * The liveness event log must never block or slow liveness. The writer thread
 * runs, as in production, but its sink is a stand-in that works, fails, or
 * blocks. Time is a fake ticker and the sweeper is off.
 */
@Testcontainers
@SpringBootTest(properties = {
        "spring.grpc.server.port=0",
        "coordinator.liveness-sweep=0",
        "coordinator.repair-interval=0",
        "coordinator.gc-interval=0",
})
class LivenessEventIT {

    static final List<String> ALL = List.of("node1", "node2", "node3", "node4");

    @Container
    @ServiceConnection
    static PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:18");

    static final FakeNodes nodes = new FakeNodes(4);
    static final FakeTicker clock = new FakeTicker();

    enum SinkMode { WORKS, FAILS, BLOCKS }

    static final AtomicReference<SinkMode> mode = new AtomicReference<>(SinkMode.WORKS);
    static final AtomicReference<CountDownLatch> unblock = new AtomicReference<>(new CountDownLatch(0));
    static final AtomicInteger blockedWrites = new AtomicInteger();

    @DynamicPropertySource
    static void registerNodes(DynamicPropertyRegistry registry) {
        registry.add("coordinator.nodes", nodes::nodesProperty);
    }

    @TestConfiguration
    static class Config {
        @Bean
        @Primary
        Ticker fakeTicker() {
            return clock;
        }

        /** The real sink, unless the test says to fail or block. */
        @Bean
        @Primary
        LivenessEventLog.Sink switchableSink(MetadataRepository repository) {
            return events -> {
                switch (mode.get()) {
                    case WORKS -> repository.insertLivenessEvents(events);
                    case FAILS -> throw new IllegalStateException("Postgres is down (test)");
                    case BLOCKS -> {
                        // A write that hangs, then completes: Postgres was slow, not down.
                        blockedWrites.incrementAndGet();
                        try {
                            unblock.get().await();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                        repository.insertLivenessEvents(events);
                    }
                }
            };
        }
    }

    private final LivenessService liveness;
    private final LivenessEventLog eventLog;
    private final JdbcClient jdbc;

    @Autowired
    LivenessEventIT(LivenessService liveness, LivenessEventLog eventLog, JdbcClient jdbc) {
        this.liveness = liveness;
        this.eventLog = eventLog;
        this.jdbc = jdbc;
    }

    /** Every node ALIVE, and every event so far written, so a test sees only its own. */
    @BeforeEach
    void allAlive() throws InterruptedException {
        mode.set(SinkMode.WORKS);
        beatAllBut(null);
        beatAllBut(null);
        eventually(() -> assertThat(eventLog.idle()).isTrue());
    }

    @AfterEach
    void release() throws InterruptedException {
        mode.set(SinkMode.WORKS);
        unblock.get().countDown();
        eventually(() -> assertThat(eventLog.idle()).isTrue());
    }

    private void beatAllBut(String skipped) {
        ALL.stream().filter(n -> !n.equals(skipped)).forEach(n -> liveness.heartbeat(n, "test-instance"));
    }

    /** One step of a scenario: move the clock, beat every node but one, sweep. */
    private record Step(String transitions, String node2State) {
    }

    private Step step(Duration advance, String silent) {
        clock.advance(advance);
        beatAllBut(silent);
        String t = liveness.sweep().stream().map(x -> x.nodeId() + ":" + x.from() + ">" + x.to())
                .reduce((a, b) -> a + "," + b).orElse("");
        return new Step(t, persistedState("node2"));
    }

    /**
     * node2 goes silent: SUSPECT after 6 s, DEAD after 15 s, then beats again
     * and returns to ALIVE after 2 beats. Each step's transitions and node2's
     * persisted state, which must be the same whatever happens to events.
     */
    private List<Step> node2Outage() {
        List<Step> steps = new ArrayList<>();
        steps.add(step(Duration.ofSeconds(5), "node2"));
        steps.add(step(Duration.ofSeconds(2), "node2"));
        steps.add(step(Duration.ofSeconds(9), "node2"));
        clock.advance(Duration.ofSeconds(1));
        liveness.heartbeat("node2", "test-instance");
        steps.add(step(Duration.ofSeconds(1), null));
        return steps;
    }

    private static final List<Step> EXPECTED = List.of(
            new Step("", "ALIVE"),
            new Step("node2:ALIVE>SUSPECT", "SUSPECT"),
            new Step("node2:SUSPECT>DEAD", "DEAD"),
            new Step("", "ALIVE"));

    private String persistedState(String node) {
        return jdbc.sql("SELECT state FROM nodes WHERE node_id = :n").param("n", node).query(String.class).single();
    }

    private long eventRows() {
        return jdbc.sql("SELECT count(*) FROM liveness_events").query(Long.class).single();
    }

    /** Waits for the writer thread, which runs on its own, up to 10 s. */
    private static void eventually(Runnable check) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (true) {
            try {
                check.run();
                return;
            } catch (AssertionError e) {
                if (System.nanoTime() > deadline) {
                    throw e;
                }
                Thread.sleep(20);
            }
        }
    }

    @Test
    void transitionsAreRecordedInOrder() throws InterruptedException {
        long before = jdbc.sql("SELECT coalesce(max(event_id), 0) FROM liveness_events").query(Long.class).single();

        assertThat(node2Outage()).isEqualTo(EXPECTED);

        eventually(() -> assertThat(jdbc.sql("""
                SELECT from_state || '>' || to_state FROM liveness_events
                WHERE node_id = 'node2' AND event_id > :before ORDER BY event_id
                """).param("before", before).query(String.class).list())
                .containsExactly("ALIVE>SUSPECT", "SUSPECT>DEAD", "DEAD>ALIVE"));
    }

    /** Mutation: record() writes through the sink on the caller's thread, so a failure escapes into liveness. */
    @Test
    void aFailingEventWriteChangesNoStateAndDelaysNoTransition() throws InterruptedException {
        mode.set(SinkMode.FAILS);
        long droppedBefore = eventLog.dropped();
        long rowsBefore = eventRows();

        List<Step> steps = assertTimeoutPreemptively(Duration.ofSeconds(2), this::node2Outage);

        assertThat(steps).isEqualTo(EXPECTED);
        eventually(() -> assertThat(eventLog.dropped() - droppedBefore).isEqualTo(3));
        assertThat(eventRows()).isEqualTo(rowsBefore);
    }

    /** Mutation: record() waits for room in the queue (put instead of offer). */
    @Test
    void aBlockedEventWriteNeverBlocksLiveness() throws InterruptedException {
        unblock.set(new CountDownLatch(1));
        mode.set(SinkMode.BLOCKS);
        int blockedBefore = blockedWrites.get();
        long droppedBefore = eventLog.dropped();
        eventLog.record("node1", State.ALIVE, State.ALIVE); // the writer takes it and blocks
        eventually(() -> assertThat(blockedWrites.get()).isGreaterThan(blockedBefore));

        // Fill the queue past capacity, then run the outage: none of it may wait.
        List<Step> steps = assertTimeoutPreemptively(Duration.ofSeconds(2), () -> {
            for (int i = 0; i < LivenessEventLog.QUEUE_CAPACITY + 100; i++) {
                eventLog.record("node1", State.ALIVE, State.SUSPECT);
            }
            return node2Outage();
        });

        assertThat(steps).isEqualTo(EXPECTED);
        assertThat(eventLog.dropped() - droppedBefore).as("the overflow and the outage's 3 events")
                .isEqualTo(100 + 3);
    }
}
