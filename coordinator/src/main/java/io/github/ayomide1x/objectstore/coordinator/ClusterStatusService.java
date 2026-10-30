package io.github.ayomide1x.objectstore.coordinator;

import java.util.List;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * What GetClusterStatus reports. Read-only: it writes no row, publishes no
 * repair hint and starts no pass, so observers polling it change nothing.
 *
 * <p>The two counts are the expensive part (under-replicated scans every
 * current chunk's replicas), so they are cached for COORDINATOR_STATUS_CACHE_TTL
 * and recomputed by at most one caller at a time: however many observers poll,
 * the scan runs at most once per TTL, and not at all when nobody asks. The
 * cache is rebuilt from Postgres and liveness on the next call, so losing it
 * on a restart loses nothing (invariant 7). Everything else is read fresh.
 */
@Service
public class ClusterStatusService {

    static final int DEFAULT_RECENT_PASSES = 10;
    static final int MAX_RECENT_PASSES = 100;
    static final int DEFAULT_RECENT_EVENTS = 20;
    static final int MAX_RECENT_EVENTS = 100;

    /** One node: its liveness now, from the tracker, and its persisted times. */
    public record NodeView(MetadataRepository.NodeRow row, LivenessTracker.State liveness) {
    }

    /** The cached counts, and when (wall clock) they were computed. */
    public record Counts(long underReplicatedChunks, long pendingDeletions, long asOfUnixMillis) {
    }

    public record Status(List<NodeView> nodes, Counts counts,
            List<MetadataRepository.RepairPassRow> lastRepairPass, List<MetadataRepository.GcPassRow> lastGcPass,
            List<MetadataRepository.RepairPassRow> recentRepairPasses,
            List<MetadataRepository.GcPassRow> recentGcPasses,
            List<MetadataRepository.LivenessEventRow> recentLivenessEvents, long livenessEventsDropped) {
    }

    private final MetadataRepository repository;
    private final RepairService repair;
    private final LivenessTracker liveness;
    private final LivenessEventLog eventLog;
    private final Ticker ticker;
    private final long cacheTtlNanos;

    // Guarded by this.
    private Counts cached;
    private long cachedAtNanos;

    public ClusterStatusService(MetadataRepository repository, RepairService repair, LivenessTracker liveness,
            LivenessEventLog eventLog, Ticker ticker, CoordinatorProperties properties) {
        this.repository = repository;
        this.repair = repair;
        this.liveness = liveness;
        this.eventLog = eventLog;
        this.ticker = ticker;
        this.cacheTtlNanos = properties.statusCacheTtl().toNanos();
    }

    /**
     * recentPasses and recentEvents: 0 means the default; larger than the
     * maximum is clamped. One read-only transaction for the fresh reads
     * (liveness comes from memory, as everywhere); the counts may be up to the
     * cache TTL older, and say when they were computed.
     */
    @Transactional(readOnly = true)
    public Status status(int recentPasses, int recentEvents) {
        int passes = clamp("recent_passes", recentPasses, DEFAULT_RECENT_PASSES, MAX_RECENT_PASSES);
        int events = clamp("recent_events", recentEvents, DEFAULT_RECENT_EVENTS, MAX_RECENT_EVENTS);
        List<NodeView> nodes = repository.listNodeRows().stream()
                .map(r -> new NodeView(r, liveness.stateOf(r.nodeId())))
                .toList();
        return new Status(nodes, counts(),
                repository.recentRepairPasses(1, false), repository.recentGcPasses(1, false),
                repository.recentRepairPasses(passes, true), repository.recentGcPasses(passes, true),
                repository.recentLivenessEvents(events), eventLog.dropped());
    }

    /** The counts, recomputed if older than the TTL. Synchronized: concurrent callers share one scan. */
    synchronized Counts counts() {
        long now = ticker.nanos();
        if (cached == null || now - cachedAtNanos >= cacheTtlNanos) {
            cached = new Counts(repair.underReplicatedChunks(), repository.countPendingDeletions(),
                    System.currentTimeMillis());
            cachedAtNanos = now;
        }
        return cached;
    }

    private static int clamp(String field, int value, int def, int max) {
        if (value < 0) {
            throw RequestException.invalid("%s %d is negative".formatted(field, value));
        }
        return value == 0 ? def : Math.min(value, max);
    }
}
