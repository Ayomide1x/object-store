package io.github.ayomide1x.objectstore.coordinator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

import io.github.ayomide1x.objectstore.coordinator.LivenessTracker.State;
import io.grpc.Status;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The liveness state machine, driven by a fake monotonic clock: no sleeping.
 * Thresholds: SUSPECT after 6 s, DEAD after 15 s, 2 recovery beats.
 */
class LivenessTrackerTest {

    /** A clock the test moves by hand. */
    static final class FakeTicker implements Ticker {
        final AtomicLong now = new AtomicLong(1_000_000_000L);

        @Override
        public long nanos() {
            return now.get();
        }

        void advance(Duration d) {
            now.addAndGet(d.toNanos());
        }
    }

    private FakeTicker clock;
    private LivenessTracker tracker;

    @BeforeEach
    void setUp() {
        clock = new FakeTicker();
        tracker = new LivenessTracker(clock, new CoordinatorProperties(
                "node1|n1:1|n1:1,node2|n2:2|n2:2", Duration.ofSeconds(2), Duration.ofSeconds(6),
                Duration.ofSeconds(15), 2, Duration.ZERO, Duration.ofSeconds(30), Duration.ZERO, 8, 2,
                Duration.ofSeconds(30), Duration.ofHours(24)));
    }

    private void after(double seconds) {
        clock.advance(Duration.ofMillis((long) (seconds * 1000)));
    }

    @Test
    void everyNodeStartsUnknownAndTheFirstBeatMakesItAlive() {
        assertThat(tracker.snapshot()).containsEntry("node1", State.UNKNOWN).containsEntry("node2", State.UNKNOWN);
        tracker.beat("node1", "a");
        assertThat(tracker.stateOf("node1")).isEqualTo(State.ALIVE);
        assertThat(tracker.stateOf("node2")).isEqualTo(State.UNKNOWN);
    }

    /** Mutation: no SUSPECT/DEAD transitions. */
    @Test
    void silenceMakesANodeSuspectThenDeadAtTheThresholds() {
        tracker.beat("node1", "a");
        after(5.9);
        tracker.sweep();
        assertThat(tracker.stateOf("node1")).isEqualTo(State.ALIVE);
        after(0.1); // 6 s
        assertThat(tracker.sweep()).extracting(LivenessTracker.Transition::to).containsExactly(State.SUSPECT);
        assertThat(tracker.stateOf("node1")).isEqualTo(State.SUSPECT);
        after(8.9); // 14.9 s
        tracker.sweep();
        assertThat(tracker.stateOf("node1")).isEqualTo(State.SUSPECT);
        after(0.1); // 15 s
        tracker.sweep();
        assertThat(tracker.stateOf("node1")).isEqualTo(State.DEAD);
    }

    @Test
    void oneBeatBringsASuspectNodeBack() {
        tracker.beat("node1", "a");
        after(7);
        tracker.sweep();
        assertThat(tracker.stateOf("node1")).isEqualTo(State.SUSPECT);
        tracker.beat("node1", "a");
        assertThat(tracker.stateOf("node1")).isEqualTo(State.ALIVE);
    }

    /** Mutation: a single beat brings a DEAD node back (no hysteresis). */
    @Test
    void aDeadNodeNeedsTwoConsecutiveBeatsToComeBack() {
        tracker.beat("node1", "a");
        after(16);
        tracker.sweep();
        assertThat(tracker.stateOf("node1")).isEqualTo(State.DEAD);

        tracker.beat("node1", "a");
        assertThat(tracker.stateOf("node1")).as("after one beat").isEqualTo(State.DEAD);
        after(2);
        tracker.beat("node1", "a");
        assertThat(tracker.stateOf("node1")).as("after two beats within 6 s").isEqualTo(State.ALIVE);
    }

    @Test
    void aGapBetweenRecoveryBeatsStartsTheCountAgain() {
        tracker.beat("node1", "a");
        after(16);
        tracker.sweep();
        tracker.beat("node1", "a"); // recovery 1
        after(7);                   // longer than the SUSPECT threshold
        tracker.beat("node1", "a"); // counts as recovery 1 again
        assertThat(tracker.stateOf("node1")).isEqualTo(State.DEAD);
        after(2);
        tracker.beat("node1", "a");
        assertThat(tracker.stateOf("node1")).isEqualTo(State.ALIVE);
    }

    /** Mutation: UNKNOWN goes DEAD before a full window has passed since start. */
    @Test
    void afterARestartANodeIsNeverDeadBeforeAFullWindow() {
        after(14.9);
        tracker.sweep();
        assertThat(tracker.snapshot()).containsEntry("node1", State.UNKNOWN).containsEntry("node2", State.UNKNOWN);
        tracker.beat("node1", "a");
        after(0.1); // 15 s since start
        tracker.sweep();
        assertThat(tracker.stateOf("node1")).isEqualTo(State.ALIVE);
        assertThat(tracker.stateOf("node2")).as("never heard from in a full window").isEqualTo(State.DEAD);
    }

    /** Mutation: a beat from an unknown node ID is accepted. */
    @Test
    void aBeatFromAnUnregisteredNodeIsRejectedAndRecordsNothing() {
        assertThatThrownBy(() -> tracker.beat("node9", "x"))
                .isInstanceOfSatisfying(RequestException.class,
                        e -> assertThat(e.code()).isEqualTo(Status.Code.NOT_FOUND))
                .hasMessageContaining("node9 is not in COORDINATOR_NODES");
        assertThat(tracker.snapshot()).containsOnlyKeys("node1", "node2");
    }

    @Test
    void aNewInstanceIsARestart() {
        tracker.beat("node1", "a");
        after(1);
        LivenessTracker.Beat beat = tracker.beat("node1", "b"); // restarted quickly
        assertThat(beat.newInstance()).isTrue();
        assertThat(beat.state()).isEqualTo(State.ALIVE);
        assertThat(tracker.instanceOf("node1")).isEqualTo("b");
    }

    /** Mutation: a conflicting instance is accepted. */
    @Test
    void twoProcessesClaimingOneNodeAreRejectedUntilOneGoesQuiet() {
        tracker.beat("node1", "a");
        after(1);
        tracker.beat("node1", "b"); // looks like a restart...
        after(1);
        // ...until the replaced instance beats again: two processes.
        assertThatThrownBy(() -> tracker.beat("node1", "a"))
                .isInstanceOfSatisfying(RequestException.class,
                        e -> assertThat(e.code()).isEqualTo(Status.Code.FAILED_PRECONDITION))
                .hasMessageContaining("claimed by 2 processes");
        after(1);
        assertThatThrownBy(() -> tracker.beat("node1", "b")).isInstanceOf(RequestException.class);

        // Rejected beats are not signs of life: the node goes SUSPECT, then DEAD.
        after(5);
        tracker.sweep();
        assertThat(tracker.stateOf("node1")).isEqualTo(State.SUSPECT);

        // "a" stops (last seen at t=2 s); "b" keeps beating but is rejected until
        // "a" has been quiet for 15 s, at t=17 s. Meanwhile the node goes DEAD
        // (last accepted beat at t=1 s).
        for (int i = 0; i < 4; i++) { // t = 10, 12, 14, 16 s
            after(2);
            assertThatThrownBy(() -> tracker.beat("node1", "b")).isInstanceOf(RequestException.class);
            tracker.sweep();
        }
        assertThat(tracker.stateOf("node1")).isEqualTo(State.DEAD);
        after(2);
        tracker.beat("node1", "b"); // t=18 s: only "b" remains; accepted, recovery 1
        assertThat(tracker.stateOf("node1")).isEqualTo(State.DEAD);
        after(2);
        tracker.beat("node1", "b"); // recovery 2
        assertThat(tracker.stateOf("node1")).isEqualTo(State.ALIVE);
        assertThat(tracker.instanceOf("node1")).isEqualTo("b");
    }
}
