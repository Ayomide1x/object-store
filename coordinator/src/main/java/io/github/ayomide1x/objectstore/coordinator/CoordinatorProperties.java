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
 * @param repairGrace    {@code COORDINATOR_REPAIR_GRACE}: how long a node must be DEAD
 *                       before its replicas stop counting and are repaired
 * @param repairInterval {@code COORDINATOR_REPAIR_INTERVAL}: time between reconciler
 *                       passes; 0 disables the scheduler (tests run passes by hand)
 * @param repairMaxInflight {@code COORDINATOR_REPAIR_MAX_INFLIGHT}: copies at once,
 *                       cluster-wide
 * @param repairMaxPerNode  {@code COORDINATOR_REPAIR_MAX_PER_NODE}: copies at once per
 *                       node, as source or target
 * @param copyTimeout    {@code COORDINATOR_COPY_TIMEOUT}: deadline for one copy
 * @param repairPassRetention {@code COORDINATOR_REPAIR_PASS_RETENTION}: how long
 *                       repair_passes rows are kept
 * @param gcInterval     {@code COORDINATOR_GC_INTERVAL}: time between garbage
 *                       collection passes; 0 disables the scheduler (tests run passes by hand)
 * @param gcReadGrace    {@code COORDINATOR_GC_READ_GRACE}: how long after an object is
 *                       superseded or removed (or a replica marked BAD) its files are kept,
 *                       so a reader that already holds the chunk list finishes
 * @param gcPendingTimeout {@code COORDINATOR_GC_PENDING_TIMEOUT}: how long an upload may
 *                       stay PENDING before it is abandoned and its files deleted
 * @param gcBatch        {@code COORDINATOR_GC_BATCH}: objects retired or scheduled per
 *                       pass step, and chunk IDs per DeleteChunks call
 * @param gcMaxInflight  {@code COORDINATOR_GC_MAX_INFLIGHT}: DeleteChunks calls at once,
 *                       cluster-wide
 * @param gcMaxPerNode   {@code COORDINATOR_GC_MAX_PER_NODE}: DeleteChunks calls at once
 *                       per node
 * @param gcPassRetention {@code COORDINATOR_GC_PASS_RETENTION}: how long gc_passes rows
 *                       are kept
 * @param statusCacheTtl {@code COORDINATOR_STATUS_CACHE_TTL}: how long GetClusterStatus
 *                       reuses its under-replicated and pending-deletion counts; 0 recomputes
 *                       them on every call
 * @param livenessEventRetention {@code COORDINATOR_LIVENESS_EVENT_RETENTION}: how long
 *                       liveness_events rows are kept
 * @param livenessEventWriter {@code COORDINATOR_LIVENESS_EVENT_WRITER}: whether the thread
 *                       that writes liveness events runs; false only in tests, which flush by hand
 */
@ConfigurationProperties("coordinator")
public record CoordinatorProperties(
        String nodes,
        @DefaultValue("2s") Duration nodeRpcTimeout,
        @DefaultValue("6s") Duration suspectAfter,
        @DefaultValue("15s") Duration deadAfter,
        @DefaultValue("2") int recoveryBeats,
        @DefaultValue("1s") Duration livenessSweep,
        @DefaultValue("30s") Duration repairGrace,
        @DefaultValue("10s") Duration repairInterval,
        @DefaultValue("8") int repairMaxInflight,
        @DefaultValue("2") int repairMaxPerNode,
        @DefaultValue("30s") Duration copyTimeout,
        @DefaultValue("24h") Duration repairPassRetention,
        @DefaultValue("10s") Duration gcInterval,
        @DefaultValue("15m") Duration gcReadGrace,
        @DefaultValue("24h") Duration gcPendingTimeout,
        @DefaultValue("256") int gcBatch,
        @DefaultValue("4") int gcMaxInflight,
        @DefaultValue("1") int gcMaxPerNode,
        @DefaultValue("24h") Duration gcPassRetention,
        @DefaultValue("1s") Duration statusCacheTtl,
        @DefaultValue("24h") Duration livenessEventRetention,
        @DefaultValue("true") boolean livenessEventWriter) {

    public CoordinatorProperties {
        if (suspectAfter.isNegative() || suspectAfter.isZero() || deadAfter.compareTo(suspectAfter) <= 0) {
            throw new IllegalStateException(
                    "need 0 < COORDINATOR_SUSPECT_AFTER (%s) < COORDINATOR_DEAD_AFTER (%s)".formatted(suspectAfter, deadAfter));
        }
        if (repairMaxInflight < 1 || repairMaxPerNode < 1) {
            throw new IllegalStateException("COORDINATOR_REPAIR_MAX_INFLIGHT and _MAX_PER_NODE must be >= 1");
        }
        if (gcReadGrace.isNegative() || gcPendingTimeout.isNegative() || gcPendingTimeout.isZero()) {
            throw new IllegalStateException("need COORDINATOR_GC_READ_GRACE >= 0 and COORDINATOR_GC_PENDING_TIMEOUT > 0");
        }
        if (gcBatch < 1 || gcMaxInflight < 1 || gcMaxPerNode < 1) {
            throw new IllegalStateException("COORDINATOR_GC_BATCH, _MAX_INFLIGHT and _MAX_PER_NODE must be >= 1");
        }
        if (statusCacheTtl.isNegative()) {
            throw new IllegalStateException("COORDINATOR_STATUS_CACHE_TTL must be >= 0, got " + statusCacheTtl);
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
