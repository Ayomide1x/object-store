package io.github.ayomide1x.objectstore.coordinator;

import java.util.ArrayList;
import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Coordinator settings, bound from environment variables.
 *
 * @param nodes {@code COORDINATOR_NODES}: comma-separated
 *              {@code node_id|internal_addr|client_addr} entries
 */
@ConfigurationProperties("coordinator")
public record CoordinatorProperties(String nodes) {

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
