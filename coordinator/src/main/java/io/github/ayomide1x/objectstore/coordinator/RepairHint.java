package io.github.ayomide1x.objectstore.coordinator;

/**
 * A hint that a reconciler pass may find work now: a replica was marked BAD, a
 * commit recorded fewer than 3 replicas, or a node became ALIVE. Passes still
 * run on their interval; correctness never depends on a hint arriving.
 */
public record RepairHint(String reason) {
}
