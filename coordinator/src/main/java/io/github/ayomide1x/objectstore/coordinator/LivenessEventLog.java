package io.github.ayomide1x.objectstore.coordinator;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import io.github.ayomide1x.objectstore.coordinator.LivenessTracker.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;

/**
 * Records liveness transitions in liveness_events, for observers, without
 * ever slowing liveness down.
 *
 * <p>{@link #record} only offers the event to a bounded queue: it never
 * blocks, never touches Postgres and never throws. A separate writer thread
 * drains the queue in batches. If the queue is full, or a batch's write
 * fails, those events are dropped: each drop is logged and counted
 * ({@link #dropped}), and liveness, which never reads this table, behaves
 * exactly as it would without it.
 */
@Component
public class LivenessEventLog implements InitializingBean, DisposableBean {

    static final int QUEUE_CAPACITY = 1024;
    private static final int BATCH = 256;
    private static final Logger log = LoggerFactory.getLogger(LivenessEventLog.class);

    /** A transition to record. from is null for the coordinator-startup events. */
    public record Event(String nodeId, State from, State to, Instant at) {
    }

    /** Where batches go: Postgres in production, a stand-in in tests. */
    @FunctionalInterface
    public interface Sink {
        void write(List<Event> events);
    }

    private final Sink sink;
    private final boolean startWriter;
    private final BlockingQueue<Event> queue = new ArrayBlockingQueue<>(QUEUE_CAPACITY);
    private final AtomicLong dropped = new AtomicLong();
    private final AtomicLong outstanding = new AtomicLong(); // queued or being written
    private Thread writer;

    public LivenessEventLog(Sink sink, CoordinatorProperties properties) {
        this.sink = sink;
        this.startWriter = properties.livenessEventWriter();
    }

    @Override
    public void afterPropertiesSet() {
        if (startWriter) {
            writer = Thread.ofPlatform().daemon().name("liveness-event-writer").start(this::writeLoop);
        }
    }

    /** Queues one event, stamped now. Never blocks; drops (and counts) the event if the queue is full. */
    public void record(String nodeId, State from, State to) {
        Event e = new Event(nodeId, from, to, Instant.now());
        outstanding.incrementAndGet();
        if (!queue.offer(e)) {
            outstanding.decrementAndGet();
            long n = dropped.incrementAndGet();
            log.atWarn().addKeyValue("node_id", nodeId).addKeyValue("from", from).addKeyValue("to", to)
                    .addKeyValue("dropped_total", n).log("liveness event dropped: queue full");
        }
    }

    /** Events not recorded since this coordinator started. */
    public long dropped() {
        return dropped.get();
    }

    /** Nothing queued and no batch being written. For tests. */
    boolean idle() {
        return outstanding.get() == 0;
    }

    /**
     * Writes every queued event now, on the calling thread. For tests that run
     * without the writer thread; the writer uses the same path.
     */
    public void flush() {
        List<Event> batch = new ArrayList<>();
        while (queue.drainTo(batch, BATCH) > 0) {
            write(batch);
            batch.clear();
        }
    }

    private void writeLoop() {
        List<Event> batch = new ArrayList<>();
        while (!Thread.currentThread().isInterrupted()) {
            try {
                batch.add(queue.take());
            } catch (InterruptedException e) {
                return;
            }
            queue.drainTo(batch, BATCH - 1);
            write(batch);
            batch.clear();
        }
    }

    private void write(List<Event> batch) {
        try {
            sink.write(batch);
        } catch (RuntimeException e) {
            long n = dropped.addAndGet(batch.size());
            log.atWarn().setCause(e).addKeyValue("events", batch.size()).addKeyValue("dropped_total", n)
                    .log("liveness events dropped: write failed");
        } finally {
            outstanding.addAndGet(-batch.size());
        }
    }

    @Override
    public void destroy() {
        if (writer != null) {
            writer.interrupt();
        }
    }
}
