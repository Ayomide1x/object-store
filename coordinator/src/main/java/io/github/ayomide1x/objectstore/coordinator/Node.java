package io.github.ayomide1x.objectstore.coordinator;

/**
 * A storage node. {@code internalAddr} is reachable inside the cluster network;
 * {@code clientAddr} is what external clients are told to dial.
 */
public record Node(String nodeId, String internalAddr, String clientAddr) {
}
