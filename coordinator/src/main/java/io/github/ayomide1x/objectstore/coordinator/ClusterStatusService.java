package io.github.ayomide1x.objectstore.coordinator;

import java.util.List;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * What GetClusterStatus reports. Read-only: it writes no row, publishes no
 * repair hint and starts no pass, so observers polling it change nothing.
 */
@Service
public class ClusterStatusService {

    static final int DEFAULT_RECENT_PASSES = 10;
    static final int MAX_RECENT_PASSES = 100;

    /** One node: its liveness now, from the tracker, and its persisted times. */
    public record NodeView(MetadataRepository.NodeRow row, LivenessTracker.State liveness) {
    }

    public record Status(List<NodeView> nodes, long underReplicatedChunks, long pendingDeletions,
            List<MetadataRepository.RepairPassRow> lastRepairPass, List<MetadataRepository.GcPassRow> lastGcPass,
            List<MetadataRepository.RepairPassRow> recentRepairPasses,
            List<MetadataRepository.GcPassRow> recentGcPasses) {
    }

    private final MetadataRepository repository;
    private final RepairService repair;
    private final LivenessTracker liveness;

    public ClusterStatusService(MetadataRepository repository, RepairService repair, LivenessTracker liveness) {
        this.repository = repository;
        this.repair = repair;
        this.liveness = liveness;
    }

    /**
     * recentPasses: 0 means DEFAULT_RECENT_PASSES; larger than MAX_RECENT_PASSES
     * is clamped. One read-only transaction, so the counts are one snapshot of
     * Postgres (liveness comes from memory, as everywhere).
     */
    @Transactional(readOnly = true)
    public Status status(int recentPasses) {
        if (recentPasses < 0) {
            throw RequestException.invalid("recent_passes %d is negative".formatted(recentPasses));
        }
        int n = recentPasses == 0 ? DEFAULT_RECENT_PASSES : Math.min(recentPasses, MAX_RECENT_PASSES);
        List<NodeView> nodes = repository.listNodeRows().stream()
                .map(r -> new NodeView(r, liveness.stateOf(r.nodeId())))
                .toList();
        return new Status(nodes, repair.underReplicatedChunks(), repository.countPendingDeletions(),
                repository.recentRepairPasses(1, false), repository.recentGcPasses(1, false),
                repository.recentRepairPasses(n, true), repository.recentGcPasses(n, true));
    }
}
