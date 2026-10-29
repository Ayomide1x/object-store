package io.github.ayomide1x.objectstore.coordinator;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Coordinator settings, bound from environment variables.
 *
 * @param nodes          {@code COORDINATOR_NODES}: comma-separated
 *                       {@code node_id|internal_addr|client_addr} entries
 * @param nodeRpcTimeout {@code COORDINATOR_NODE_RPC_TIMEOUT}: deadline for each
 *                       call to a storage node, such as {@code 2s}
 * @param suspectAfter   {@code COORDINATOR_SUSPECT_AFTER}: silence after which an
 *                       ALIVE node becomes SUSPECT
 * @param deadAfter      {@code COORDINATOR_DEAD_AFTER}: silence after which a node
 *                       becomes DEAD
 * @param recoveryBeats  {@code COORDINATOR_RECOVERY_BEATS}: consecutive beats, each
 *                       within suspectAfter of the last, that bring a DEAD node back
 * @param livenessSweep  {@code COORDINATOR_LIVENESS_SWEEP}: how often timeouts are
 *                       applied; 0 disables the sweeper (tests sweep by hand)
 */
@ConfigurationProperties("coordinator")
public record CoordinatorProperties(
        String nodes,
        @DefaultValue("2s") Duration nodeRpcTimeout,
        @DefaultValue("6s") Duration suspectAfter,
        @DefaultValue("15s") Duration deadAfter,
        @DefaultValue("2") int recoveryBeats,
        @DefaultValue("1s") Duration livenessSweep) {

    public CoordinatorProperties {
        if (suspectAfter.isNegative() || suspectAfter.isZero() || deadAfter.compareTo(suspectAfter) <= 0) {
            throw new IllegalStateException(
                    "need 0 < COORDINATOR_SUSPECT_AFTER (%s) < COORDINATOR_DEAD_AFTER (%s)".formatted(suspectAfter, deadAfter));
        }
        if (recoveryBeats < 1) {
            throw new IllegalStateException("COORDINATOR_RECOVERY_BEATS must be >= 1, got " + recoveryBeats);
        }
    }

    public List<Node> parseNodes() {
        if (nodes == null || nodes.isBlank()) {
            throw new IllegalStateException("COORDINATOR_NODES is required");
        }
        List<Node> result = new ArrayList<>();
        for (String entry : nodes.split(",")) {
            String[] parts = entry.trim().split("\\|");
            if (parts.length != 3 || parts[0].isBlank() || parts[1].isBlank() || parts[2].isBlank()) {
                throw new IllegalStateException(
                        "COORDINATOR_NODES entry '%s' must be node_id|internal_addr|client_addr".formatted(entry));
            }
            result.add(new Node(parts[0], parts[1], parts[2]));
        }
        return result;
    }
}
