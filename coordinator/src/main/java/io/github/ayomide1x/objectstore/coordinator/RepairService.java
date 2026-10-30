package io.github.ayomide1x.objectstore.coordinator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.github.ayomide1x.objectstore.coordinator.LivenessTracker.State;
import io.github.ayomide1x.objectstore.coordinator.MetadataRepository.RepairChunk;
import io.github.ayomide1x.objectstore.coordinator.MetadataRepository.ReplicaRow;
import io.github.ayomide1x.objectstore.v1.ChunkStat;
import io.github.ayomide1x.objectstore.v1.PushChunkResponse;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * The reconciler: finds chunks of current COMMITTED objects with fewer than 3
 * counted replicas and orders node-to-node copies to bring them back.
 *
 * <p>A replica counts if its row is HEALTHY and its node is ALIVE, SUSPECT, or
 * DEAD for less than the repair grace period. BAD rows never count. While any
 * node is UNKNOWN (state not yet rebuilt after a restart) a pass does nothing.
 *
 * <p>Each pass recomputes everything from Postgres and liveness: there is no
 * task table. Passes never overlap (one scheduler thread; a pass waits for its
 * copies), so a pass sees every earlier copy's outcome, and a pass right after
 * a complete one copies nothing. A new replica row is written only after the
 * target itself, answering as itself, reports a verified copy (StatChunks).
 *
 * <p>Repair never crosses garbage collection. Before ordering a copy it records
 * the target as a placement, and both that and the replica row afterwards are
 * written only under a share lock on the object row that finds the object still
 * current ({@link MetadataRepository#lockIfCurrent}). So a copy is never
 * started, or recorded, for an object being deleted, and every copy that was
 * started is a placement GC will delete from.
 */
@Service
public class RepairService implements InitializingBean, DisposableBean {

    static final int TARGET_REPLICAS = 3;
    private static final int MAX_SOURCE_ATTEMPTS = 3;

    private static final Logger log = LoggerFactory.getLogger(RepairService.class);

    /** What a pass did, as also written to repair_passes. */
    public record PassResult(String skippedReason, int underReplicated, int copiesStarted, int recorded,
            int foundRecorded, int badFound, int failures) {
    }

    private final MetadataRepository repository;
    private final NodeClient nodeClient;
    private final LivenessTracker liveness;
    private final ReplicaHealthService replicaHealth;
    private final CoordinatorProperties properties;
    private final TransactionTemplate tx;
    private final LinkedBlockingQueue<RepairHint> hints = new LinkedBlockingQueue<>();
    private Thread scheduler;

    public RepairService(MetadataRepository repository, NodeClient nodeClient, LivenessTracker liveness,
            ReplicaHealthService replicaHealth, CoordinatorProperties properties, TransactionTemplate tx) {
        this.repository = repository;
        this.nodeClient = nodeClient;
        this.liveness = liveness;
        this.replicaHealth = replicaHealth;
        this.properties = properties;
        this.tx = tx;
    }

    /**
     * Whether a HEALTHY row on a node in state s counts as a replica: its node
     * is ALIVE or SUSPECT, or DEAD for less than the repair grace. Repair and
     * garbage collection share this rule, so GC never treats a long-DEAD copy
     * as one of the replicas that make deleting a BAD file safe.
     */
    static boolean counts(State s, long deadForNanos, long graceNanos) {
        return switch (s) {
            case ALIVE, SUSPECT -> true;
            case DEAD -> deadForNanos < graceNanos;
            case UNKNOWN -> true; // unreachable: passes are skipped while any node is UNKNOWN
        };
    }

    /** The replicas of c that count: HEALTHY rows whose node passes {@link #counts}, given states. */
    private int countedReplicas(RepairChunk c, Map<String, State> states) {
        long graceNanos = properties.repairGrace().toNanos();
        int counted = 0;
        for (ReplicaRow r : c.replicas()) {
            if (r.state().equals("HEALTHY")
                    && counts(states.getOrDefault(r.nodeId(), State.UNKNOWN), liveness.deadForNanos(r.nodeId()), graceNanos)) {
                counted++;
            }
        }
        return counted;
    }

    /**
     * Chunks of current objects with fewer than TARGET_REPLICAS counted
     * replicas, now: what a pass would find to repair. Read-only, and not
     * synchronized with passes; for GetClusterStatus. Unlike a pass it still
     * counts while a node is UNKNOWN (such a node's rows count as replicas).
     */
    public long underReplicatedChunks() {
        Map<String, State> states = liveness.snapshot();
        return repository.repairCandidates().stream()
                .filter(c -> countedReplicas(c, states) < TARGET_REPLICAS)
                .count();
    }

    @Override
    public void afterPropertiesSet() {
        if (properties.repairInterval().isZero()) {
            return; // tests run passes by hand
        }
        scheduler = Thread.ofPlatform().daemon().name("repair-reconciler").start(this::scheduleLoop);
    }

    /** Runs a pass every interval, or sooner when a hint arrives (at most one extra pass per second). */
    private void scheduleLoop() {
        long intervalMs = properties.repairInterval().toMillis();
        while (!Thread.currentThread().isInterrupted()) {
            try {
                RepairHint hint = hints.poll(intervalMs, TimeUnit.MILLISECONDS);
                if (hint != null) {
                    Thread.sleep(1000); // debounce: let related hints pile up
                    hints.clear();
                }
                runPass();
            } catch (InterruptedException e) {
                return;
            } catch (RuntimeException e) {
                log.error("repair pass failed", e);
            }
        }
    }

    @EventListener
    public void onHint(RepairHint hint) {
        hints.offer(hint);
    }

    /** Runs one reconciler pass. Synchronized: passes never overlap. */
    public synchronized PassResult runPass() {
        repository.pruneRepairPasses(properties.repairPassRetention());
        long passId = repository.startRepairPass();

        Map<String, State> states = liveness.snapshot();
        List<String> unknown = states.entrySet().stream().filter(e -> e.getValue() == State.UNKNOWN)
                .map(Map.Entry::getKey).toList();
        if (!unknown.isEmpty()) {
            String reason = "nodes UNKNOWN: " + unknown;
            repository.finishRepairPass(passId, reason, 0, 0, 0, 0, 0, 0);
            log.atInfo().addKeyValue("pass_id", passId).log("repair pass skipped: {}", reason);
            return new PassResult(reason, 0, 0, 0, 0, 0, 0);
        }

        Map<String, Node> nodes = new LinkedHashMap<>();
        repository.listNodes().forEach(n -> nodes.put(n.nodeId(), n));

        // Find what needs work, most at risk first.
        record Need(RepairChunk chunk, int counted, List<String> sources, Set<String> withRow) {
        }
        List<Need> needs = new ArrayList<>();
        for (RepairChunk c : repository.repairCandidates()) {
            int counted = countedReplicas(c, states);
            List<String> sources = new ArrayList<>();
            Set<String> withRow = new HashSet<>();
            for (ReplicaRow r : c.replicas()) {
                withRow.add(r.nodeId());
                if (r.state().equals("HEALTHY") && states.getOrDefault(r.nodeId(), State.UNKNOWN) == State.ALIVE) {
                    sources.add(r.nodeId());
                }
            }
            if (counted >= TARGET_REPLICAS) {
                continue;
            }
            if (sources.isEmpty()) {
                log.atError().addKeyValue("chunk_id", c.chunkId()).addKeyValue("object_id", c.objectId())
                        .log("chunk has no healthy replica on an ALIVE node; cannot repair");
            }
            needs.add(new Need(c, counted, sources, withRow));
        }
        needs.sort(Comparator.comparingInt(Need::counted)
                .thenComparing(n -> n.chunk().objectCreated())
                .thenComparing(n -> n.chunk().chunkId()));

        // Run copies within the caps, refilling freed slots as copies finish:
        // the caps bound copies in flight, not copies per pass. The pass ends
        // when nothing is in flight and nothing more can be scheduled, so the
        // next pass sees every outcome.
        Load load = new Load(properties.repairMaxInflight(), properties.repairMaxPerNode());
        List<String> aliveNodes = states.entrySet().stream().filter(e -> e.getValue() == State.ALIVE)
                .map(Map.Entry::getKey).sorted().toList();
        Map<UUID, Set<String>> excludedBy = new HashMap<>();
        Map<UUID, Integer> remaining = new HashMap<>();
        for (Need need : needs) {
            excludedBy.put(need.chunk().chunkId(), new HashSet<>(need.withRow()));
            remaining.put(need.chunk().chunkId(), TARGET_REPLICAS - need.counted());
        }
        Counters n = new Counters();
        int started = 0;
        try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
            java.util.concurrent.ExecutorCompletionService<Void> done = new java.util.concurrent.ExecutorCompletionService<>(pool);
            int inFlight = 0;
            while (true) {
                // Schedule everything that fits now, most at risk first.
                for (Need need : needs) {
                    UUID id = need.chunk().chunkId();
                    Set<String> excluded = excludedBy.get(id);
                    while (remaining.get(id) > 0 && !load.full()) {
                        // Never a node with any row for the chunk (a BAD row's file
                        // blocks the no-replace link until GC deletes it, then the row).
                        String target = aliveNodes.stream().filter(x -> !excluded.contains(x) && load.has(x))
                                .min(Comparator.comparingInt(load::of).thenComparing(x -> x)).orElse(null);
                        String source = need.sources().stream().filter(load::has)
                                .min(Comparator.comparingInt(load::of).thenComparing(x -> x)).orElse(null);
                        if (target == null || source == null || !load.reserve(source, target)) {
                            break;
                        }
                        excluded.add(target);
                        remaining.merge(id, -1, Integer::sum);
                        List<String> ordered = new ArrayList<>();
                        ordered.add(source);
                        need.sources().stream().filter(s -> !s.equals(source)).forEach(ordered::add);
                        Node targetNode = nodes.get(target);
                        done.submit(() -> {
                            try {
                                copyOne(need.chunk(), targetNode, ordered, nodes, load, n);
                            } catch (RuntimeException e) {
                                n.failures.incrementAndGet();
                                log.atError().addKeyValue("chunk_id", id).log("copy failed", e);
                            } finally {
                                load.release(source, target);
                            }
                            return null;
                        });
                        inFlight++;
                        started++;
                    }
                }
                if (inFlight == 0) {
                    break; // nothing running, and nothing more fits: done
                }
                try {
                    done.take(); // a copy finished: its slots are free again
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
                inFlight--;
            }
        }

        PassResult result = new PassResult(null, needs.size(), started, n.recorded.get(), n.found.get(),
                n.bad.get(), n.failures.get());
        repository.finishRepairPass(passId, null, result.underReplicated(), result.copiesStarted(), result.recorded(),
                result.foundRecorded(), result.badFound(), result.failures());
        if (result.underReplicated() > 0 || result.copiesStarted() > 0) {
            log.atInfo().addKeyValue("pass_id", passId).addKeyValue("under_replicated", result.underReplicated())
                    .addKeyValue("copies", result.copiesStarted()).addKeyValue("recorded", result.recorded())
                    .addKeyValue("found", result.foundRecorded()).addKeyValue("bad_found", result.badFound())
                    .addKeyValue("failures", result.failures()).log("repair pass");
        }
        return result;
    }

    /** One copy of chunk onto target, trying sources in order. Records only what the target confirms. */
    private void copyOne(RepairChunk chunk, Node target, List<String> sources, Map<String, Node> nodes, Load load,
            Counters n) {
        var logCtx = log.atInfo().addKeyValue("chunk_id", chunk.chunkId()).addKeyValue("object_id", chunk.objectId())
                .addKeyValue("target_node_id", target.nodeId());
        // The placement first, so GC finds the target's file whatever happens
        // next; and only while the object is current.
        Boolean placed = tx.execute(status -> {
            if (!repository.lockIfCurrent(chunk.objectId())) {
                return false;
            }
            repository.insertRepairPlacement(chunk.chunkId(), target.nodeId());
            return true;
        });
        if (!Boolean.TRUE.equals(placed)) {
            logCtx.log("object no longer current (removed or superseded); copy not started");
            return;
        }
        for (int i = 0; i < Math.min(MAX_SOURCE_ATTEMPTS, sources.size()); i++) {
            String sourceId = sources.get(i);
            boolean extraSlot = i > 0;
            if (extraSlot && !load.reserveSource(sourceId)) {
                continue;
            }
            try {
                PushChunkResponse resp = nodeClient.pushChunk(nodes.get(sourceId), chunk.chunkId(), chunk.sha256(), target);
                switch (resp.getOutcome()) {
                    case OUTCOME_COPIED -> {
                        confirm(chunk, target, "repair", n);
                        return;
                    }
                    case OUTCOME_TARGET_HAS_CHUNK -> {
                        // Not retried as a copy: the target's own report decides.
                        confirm(chunk, target, "repair-found", n);
                        return;
                    }
                    default -> {
                        n.failures.incrementAndGet();
                        logCtx.addKeyValue("node_id", sourceId).log("copy to target failed: {}", resp.getDetail());
                        return;
                    }
                }
            } catch (StatusRuntimeException e) {
                Status.Code code = e.getStatus().getCode();
                if (code == Status.Code.DATA_LOSS || code == Status.Code.NOT_FOUND) {
                    // The stage 5 path: the source is asked to confirm before it is marked.
                    String reason = code == Status.Code.DATA_LOSS ? "REASON_DATA_LOSS" : "REASON_NOT_FOUND";
                    replicaHealth.reportBadReplica(chunk.chunkId(), sourceId, reason,
                            "repair source: " + e.getStatus().getDescription());
                }
                logCtx.addKeyValue("node_id", sourceId).log("repair source failed ({}); trying the next", code);
            } finally {
                if (extraSlot) {
                    load.releaseSource(sourceId);
                }
            }
        }
        n.failures.incrementAndGet();
    }

    /**
     * Asks the target itself what it holds (StatChunks, identity-checked) and
     * records only that: a verified copy becomes a HEALTHY row; an invalid file
     * a BAD row; anything else records nothing.
     */
    private void confirm(RepairChunk chunk, Node target, String origin, Counters n) {
        Map<String, Map<UUID, ChunkStat>> stats = nodeClient.statChunks(chunk.objectId(),
                Map.of(target, List.of(chunk.chunkId())));
        ChunkStat s = stats.getOrDefault(target.nodeId(), Map.of()).get(chunk.chunkId());
        var logCtx = log.atInfo().addKeyValue("chunk_id", chunk.chunkId()).addKeyValue("object_id", chunk.objectId())
                .addKeyValue("node_id", target.nodeId());
        if (s == null || s.getState() == ChunkStat.State.STATE_MISSING) {
            n.failures.incrementAndGet();
            logCtx.log("target did not confirm a copy; nothing recorded");
            return;
        }
        boolean valid = s.getState() == ChunkStat.State.STATE_FOUND && s.getSizeBytes() == chunk.sizeBytes()
                && Arrays.equals(s.getSha256().toByteArray(), chunk.sha256());
        if (valid) {
            Integer inserted = tx.execute(status -> repository.lockIfCurrent(chunk.objectId())
                    ? repository.insertRepairedReplica(chunk.chunkId(), target.nodeId(), origin) : -1);
            if (inserted != null && inserted < 0) {
                logCtx.log("object no longer current; copy not recorded (GC deletes it through its placement)");
            } else if (inserted != null && inserted > 0) {
                (origin.equals("repair") ? n.recorded : n.found).incrementAndGet();
                logCtx.addKeyValue("origin", origin).log("replica recorded after the target confirmed it");
            }
            return;
        }
        String reason = "invalid file found during repair: " + (s.getState() == ChunkStat.State.STATE_FOUND
                ? "size or sha256 differs from the record" : "bad header");
        Integer badInserted = tx.execute(status -> repository.lockIfCurrent(chunk.objectId())
                ? repository.insertBadReplicaFound(chunk.chunkId(), target.nodeId(), reason) : 0);
        if (badInserted != null && badInserted > 0) {
            n.bad.incrementAndGet();
            log.atWarn().addKeyValue("chunk_id", chunk.chunkId()).addKeyValue("node_id", target.nodeId())
                    .log("target holds an invalid file under the chunk's name; recorded BAD");
        }
    }

    /** Copies in flight this pass: cluster-wide and per node (as source or target). */
    private static final class Load {
        private final int maxTotal;
        private final int maxPerNode;
        private final Map<String, Integer> perNode = new HashMap<>();
        private int total;

        Load(int maxTotal, int maxPerNode) {
            this.maxTotal = maxTotal;
            this.maxPerNode = maxPerNode;
        }

        synchronized boolean full() {
            return total >= maxTotal;
        }

        synchronized boolean has(String node) {
            return perNode.getOrDefault(node, 0) < maxPerNode;
        }

        synchronized int of(String node) {
            return perNode.getOrDefault(node, 0);
        }

        synchronized boolean reserve(String source, String target) {
            if (total >= maxTotal || !has(source) || !has(target)) {
                return false;
            }
            total++;
            perNode.merge(source, 1, Integer::sum);
            perNode.merge(target, 1, Integer::sum);
            return true;
        }

        synchronized boolean reserveSource(String source) {
            if (!has(source)) {
                return false;
            }
            perNode.merge(source, 1, Integer::sum);
            return true;
        }

        synchronized void releaseSource(String source) {
            perNode.merge(source, -1, Integer::sum);
        }

        synchronized void release(String source, String target) {
            total--;
            perNode.merge(source, -1, Integer::sum);
            perNode.merge(target, -1, Integer::sum);
        }
    }

    private static final class Counters {
        final AtomicInteger recorded = new AtomicInteger();
        final AtomicInteger found = new AtomicInteger();
        final AtomicInteger bad = new AtomicInteger();
        final AtomicInteger failures = new AtomicInteger();
    }

    @Override
    public void destroy() {
        if (scheduler != null) {
            scheduler.interrupt();
        }
    }
}
