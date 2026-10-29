package io.github.ayomide1x.objectstore.coordinator;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import io.github.ayomide1x.objectstore.coordinator.LivenessTracker.State;
import io.github.ayomide1x.objectstore.coordinator.LivenessTracker.Transition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Service;

/**
 * Receives heartbeats, runs the timeout sweeper, and persists every liveness
 * transition to the nodes table (for observability, scripts and stage 7). The
 * tracker in memory stays authoritative.
 */
@Service
public class LivenessService implements InitializingBean, DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(LivenessService.class);
    private static final long HEARTBEAT_PERSIST_EVERY = Duration.ofSeconds(10).toNanos();

    private final LivenessTracker tracker;
    private final MetadataRepository repository;
    private final Ticker ticker;
    private final Duration sweepEvery;
    private final Map<String, Long> lastPersistedBeat = new ConcurrentHashMap<>();
    private ScheduledExecutorService sweeper;

    public LivenessService(LivenessTracker tracker, MetadataRepository repository, Ticker ticker,
            CoordinatorProperties properties, NodeRegistry registry) {
        this.tracker = tracker;
        this.repository = repository;
        this.ticker = ticker;
        this.sweepEvery = properties.livenessSweep();
        // registry is a dependency only so the nodes table is seeded first.
    }

    @Override
    public void afterPropertiesSet() {
        // After a restart nothing is known: say so, rather than leave stale states.
        repository.markAllNodesUnknown();
        if (!sweepEvery.isZero()) {
            sweeper = Executors.newSingleThreadScheduledExecutor(r -> Thread.ofPlatform().daemon().name("liveness-sweep").unstarted(r));
            sweeper.scheduleWithFixedDelay(this::sweepSafely, sweepEvery.toMillis(), sweepEvery.toMillis(),
                    TimeUnit.MILLISECONDS);
        }
    }

    /** Records a heartbeat; persists any transition it causes. */
    public State heartbeat(String nodeId, String instanceId) {
        LivenessTracker.Beat beat = tracker.beat(nodeId, instanceId);
        if (beat.newInstance()) {
            log.atInfo().addKeyValue("node_id", nodeId).addKeyValue("instance_id", instanceId)
                    .log("node restarted: new instance");
            repository.recordNodeInstance(nodeId, instanceId);
        }
        if (beat.transition() != null) {
            persist(beat.transition());
        }
        long now = ticker.nanos();
        Long last = lastPersistedBeat.get(nodeId);
        if (last == null || now - last >= HEARTBEAT_PERSIST_EVERY || beat.transition() != null) {
            repository.recordHeartbeat(nodeId, instanceId);
            lastPersistedBeat.put(nodeId, now);
        }
        return beat.state();
    }

    /** Applies timeouts now. Called by the sweeper, and directly by tests. */
    public List<Transition> sweep() {
        List<Transition> transitions = tracker.sweep();
        transitions.forEach(this::persist);
        return transitions;
    }

    public State stateOf(String nodeId) {
        return tracker.stateOf(nodeId);
    }

    private void persist(Transition t) {
        String silence = t.silenceNanos() == Long.MAX_VALUE ? "never" : "%.1fs".formatted(t.silenceNanos() / 1e9);
        log.atInfo().addKeyValue("node_id", t.nodeId()).addKeyValue("from", t.from()).addKeyValue("to", t.to())
                .addKeyValue("since_last_beat", silence).log("node liveness changed");
        repository.updateNodeState(t.nodeId(), t.to().name());
    }

    private void sweepSafely() {
        try {
            sweep();
        } catch (RuntimeException e) {
            log.error("liveness sweep failed", e);
        }
    }

    @Override
    public void destroy() {
        if (sweeper != null) {
            sweeper.shutdownNow();
        }
    }
}
