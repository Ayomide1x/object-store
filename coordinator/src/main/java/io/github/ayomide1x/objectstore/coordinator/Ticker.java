package io.github.ayomide1x.objectstore.coordinator;

/**
 * A monotonic clock in nanoseconds, injectable so liveness can be tested
 * without sleeping. Production uses {@link System#nanoTime()}.
 */
@FunctionalInterface
public interface Ticker {
    long nanos();
}
