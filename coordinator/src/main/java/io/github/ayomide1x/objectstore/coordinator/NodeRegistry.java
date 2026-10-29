package io.github.ayomide1x.objectstore.coordinator;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Seeds the nodes table from COORDINATOR_NODES at startup, before the gRPC
 * server accepts requests. Stage 6 replaces this with heartbeat registration.
 */
@Component
public class NodeRegistry implements InitializingBean {

    private static final Logger log = LoggerFactory.getLogger(NodeRegistry.class);

    private final CoordinatorProperties properties;
    private final MetadataRepository repository;
    private final TransactionTemplate tx;

    public NodeRegistry(CoordinatorProperties properties, MetadataRepository repository, TransactionTemplate tx) {
        this.properties = properties;
        this.repository = repository;
        this.tx = tx;
    }

    @Override
    public void afterPropertiesSet() {
        List<Node> nodes = properties.parseNodes();
        tx.executeWithoutResult(status -> nodes.forEach(repository::upsertNode));
        for (Node node : nodes) {
            log.atInfo()
                    .addKeyValue("node_id", node.nodeId())
                    .addKeyValue("internal_addr", node.internalAddr())
                    .addKeyValue("client_addr", node.clientAddr())
                    .log("node registered");
        }
    }
}
