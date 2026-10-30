package io.github.ayomide1x.objectstore.coordinator;

import java.time.Clock;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import io.github.ayomide1x.objectstore.coordinator.LivenessTracker.State;
import io.github.ayomide1x.objectstore.coordinator.MetadataRepository.BadReplica;
import io.github.ayomide1x.objectstore.coordinator.MetadataRepository.DeletionTask;
import io.github.ayomide1x.objectstore.coordinator.MetadataRepository.ObjectRow;
import io.github.ayomide1x.objectstore.coordinator.MetadataRepository.Status;
import io.github.ayomide1x.objectstore.v1.ChunkDeletionResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Garbage collection. Each pass, in order:
 *
 * <ol>
 *   <li><b>Retire.</b> PENDING uploads older than the pending timeout become
 *       ABANDONED, and COMMITTED objects no key points to become SUPERSEDED.
 *       Each is one transaction under the object's row lock, the lock
 *       CompleteUpload's commit phase takes, so a late complete either commits
 *       first (and GC skips the object) or finds it ABANDONED and fails.
 *   <li><b>Schedule.</b> Retired objects past their grace (ABANDONED at once;
 *       SUPERSEDED and DELETED after the read grace, so readers that already
 *       hold the chunk list finish) get a durable tombstone task for every
 *       (chunk, node) in their placements and replica rows. A BAD replica of a
 *       live chunk, marked longer than the read grace ago, gets a task without
 *       a tombstone, but only while the chunk keeps at least 2 counted HEALTHY
 *       replicas (repair's rule), so the last copies are never the ones removed.
 *   <li><b>Delete.</b> Tasks are sent to ALIVE nodes in batched DeleteChunks
 *       calls within the caps. A task, with that node's replica row and
 *       placement, is removed only when the node, answering as itself,
 *       reports the file deleted or absent (invariant 3). A non-ALIVE node's
 *       tasks wait; it is retried every pass once ALIVE.
 *   <li><b>Finish.</b> A retired object with nothing left on any node loses its
 *       chunks and objects rows.
 * </ol>
 *
 * Everything is recomputed from Postgres each pass, and the tasks are rows, so
 * a restart mid-GC resumes where it was (invariant 7). Like repair, a pass does
 * nothing while any node is UNKNOWN.
 */
@Service
public class GcService implements InitializingBean, DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(GcService.class);

    /** What a pass did, as also written to gc_passes. */
    public record PassResult(String skippedReason, int abandoned, int superseded, int tasksCreated, int filesDeleted,
            int alreadyAbsent, int failures, int objectsRemoved) {
    }

    private final MetadataRepository repository;
    private final NodeClient nodeClient;
    private final LivenessTracker liveness;
    private final CoordinatorProperties properties;
    private final TransactionTemplate tx;
    private final Clock clock;
    private Thread scheduler;

    public GcService(MetadataRepository repository, NodeClient nodeClient, LivenessTracker liveness,
            CoordinatorProperties properties, TransactionTemplate tx, Clock clock) {
        this.repository = repository;
        this.nodeClient = nodeClient;
        this.liveness = liveness;
        this.properties = properties;
        this.tx = tx;
        this.clock = clock;
    }

    @Override
    public void afterPropertiesSet() {
        if (properties.gcInterval().isZero()) {
            return; // tests run passes by hand
        }
        scheduler = Thread.ofPlatform().daemon().name("garbage-collector").start(this::scheduleLoop);
    }

    private void scheduleLoop() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                Thread.sleep(properties.gcInterval().toMillis());
                runPass();
            } catch (InterruptedException e) {
                return;
            } catch (RuntimeException e) {
                log.error("gc pass failed", e);
            }
        }
    }

    /** Runs one pass. Synchronized: passes never overlap. */
    public synchronized PassResult runPass() {
        repository.pruneGcPasses(properties.gcPassRetention());
        long passId = repository.startGcPass();

        Map<String, State> states = liveness.snapshot();
        List<String> unknown = states.entrySet().stream().filter(e -> e.getValue() == State.UNKNOWN)
                .map(Map.Entry::getKey).sorted().toList();
        if (!unknown.isEmpty()) {
            String reason = "nodes UNKNOWN: " + unknown;
            repository.finishGcPass(passId, reason, 0, 0, 0, 0, 0, 0, 0);
            log.atInfo().addKeyValue("pass_id", passId).log("gc pass skipped: {}", reason);
            return new PassResult(reason, 0, 0, 0, 0, 0, 0, 0);
        }

        OffsetDateTime now = OffsetDateTime.ofInstant(clock.instant(), ZoneOffset.UTC);
        int abandoned = abandon(now);
        int superseded = supersede(now);
        int tasks = scheduleRetired(now) + scheduleBadReplicas(now, states);
        Deletes d = sendDeletes(states);
        int removed = finish();

        PassResult result = new PassResult(null, abandoned, superseded, tasks, d.deleted.get(), d.absent.get(),
                d.failures.get(), removed);
        repository.finishGcPass(passId, null, abandoned, superseded, tasks, result.filesDeleted(),
                result.alreadyAbsent(), result.failures(), removed);
        if (abandoned + superseded + tasks + d.deleted.get() + d.absent.get() + d.failures.get() + removed > 0) {
            log.atInfo().addKeyValue("pass_id", passId).addKeyValue("abandoned", abandoned)
                    .addKeyValue("superseded", superseded).addKeyValue("tasks_created", tasks)
                    .addKeyValue("files_deleted", d.deleted.get()).addKeyValue("already_absent", d.absent.get())
                    .addKeyValue("failures", d.failures.get()).addKeyValue("objects_removed", removed)
                    .log("gc pass");
        }
        return result;
    }

    /**
     * PENDING longer than the pending timeout → ABANDONED, with its deletion
     * tasks, in one transaction under the row lock. No reader ever had its
     * chunk list, so there's no grace.
     */
    private int abandon(OffsetDateTime now) {
        OffsetDateTime cutoff = now.minus(properties.gcPendingTimeout());
        int n = 0;
        for (UUID id : repository.abandonCandidates(cutoff, properties.gcBatch())) {
            Boolean done = tx.execute(status -> {
                ObjectRow o = repository.lockObject(id).orElse(null);
                if (o == null || o.status() != Status.PENDING || !repository.createdBefore(id, cutoff)) {
                    return false; // completed (or gone) since the candidate query
                }
                repository.retire(id, Status.PENDING, Status.ABANDONED, now);
                repository.insertObjectDeletionTasks(id, "abandoned");
                repository.markGcStarted(id, now);
                return true;
            });
            if (Boolean.TRUE.equals(done)) {
                n++;
                log.atWarn().addKeyValue("object_id", id).log("upload abandoned: PENDING longer than the pending timeout");
            }
        }
        return n;
    }

    /**
     * COMMITTED and unreferenced → SUPERSEDED, stamped now. A key never moves
     * back to an object it left, so the condition can't change once true; it
     * is still re-checked under the row lock.
     */
    private int supersede(OffsetDateTime now) {
        int n = 0;
        for (UUID id : repository.supersedeCandidates(properties.gcBatch())) {
            Boolean done = tx.execute(status -> {
                ObjectRow o = repository.lockObject(id).orElse(null);
                if (o == null || o.status() != Status.COMMITTED || repository.isPointedTo(id)) {
                    return false;
                }
                return repository.retire(id, Status.COMMITTED, Status.SUPERSEDED, now) == 1;
            });
            if (Boolean.TRUE.equals(done)) {
                n++;
                log.atInfo().addKeyValue("object_id", id).log("object superseded; files kept for the read grace");
            }
        }
        return n;
    }

    /** Tombstone tasks for retired objects past their grace. Idempotent. */
    private int scheduleRetired(OffsetDateTime now) {
        OffsetDateTime cutoff = now.minus(properties.gcReadGrace());
        int n = 0;
        for (UUID id : repository.scheduleCandidates(cutoff, properties.gcBatch())) {
            Integer created = tx.execute(status -> {
                ObjectRow o = repository.lockObject(id).orElse(null);
                if (o == null || !o.status().retired()) {
                    return 0;
                }
                int c = repository.insertObjectDeletionTasks(id, o.status().name().toLowerCase());
                repository.markGcStarted(id, now);
                return c;
            });
            if (created != null && created > 0) {
                n += created;
                log.atInfo().addKeyValue("object_id", id).addKeyValue("tasks", created)
                        .log("deletion tasks created for a retired object");
            }
        }
        return n;
    }

    /**
     * Tasks without a tombstone for BAD replicas of live chunks, once marked
     * longer than the read grace ago, and only while the chunk has at least
     * {@link MetadataService#MIN_REPLICAS} counted HEALTHY replicas. Counted the
     * way repair counts: a HEALTHY row on a node DEAD past the repair grace
     * doesn't count. With fewer, the BAD file stays as evidence and as one of
     * the last copies; repair tops the chunk up elsewhere first.
     */
    private int scheduleBadReplicas(OffsetDateTime now, Map<String, State> states) {
        OffsetDateTime cutoff = now.minus(properties.gcReadGrace());
        long graceNanos = properties.repairGrace().toNanos();
        int n = 0;
        for (BadReplica b : repository.badReplicaCandidates(cutoff, properties.gcBatch())) {
            long counted = b.healthyNodeIds().stream()
                    .filter(id -> RepairService.counts(states.getOrDefault(id, State.UNKNOWN),
                            liveness.deadForNanos(id), graceNanos))
                    .count();
            if (counted < MetadataService.MIN_REPLICAS) {
                continue;
            }
            if (repository.insertBadReplicaTask(b.chunkId(), b.nodeId()) > 0) {
                n++;
                log.atInfo().addKeyValue("object_id", b.objectId()).addKeyValue("chunk_id", b.chunkId())
                        .addKeyValue("node_id", b.nodeId()).addKeyValue("healthy_counted", counted)
                        .log("deletion task created for a BAD replica's file");
            }
        }
        return n;
    }

    private static final class Deletes {
        final AtomicInteger deleted = new AtomicInteger();
        final AtomicInteger absent = new AtomicInteger();
        final AtomicInteger failures = new AtomicInteger();
    }

    /**
     * Sends every ALIVE node its tasks, in batches of at most gcBatch chunk IDs,
     * with at most gcMaxPerNode calls in flight per node and gcMaxInflight
     * overall. Rounds repeat until every ALIVE node's tasks are done; a node
     * that fails, or whose batch confirms nothing, is left until the next pass,
     * so every round removes rows or drops a node, and the pass ends.
     */
    private Deletes sendDeletes(Map<String, State> states) {
        Deletes d = new Deletes();
        Map<String, Node> nodes = new LinkedHashMap<>();
        repository.listNodes().forEach(node -> nodes.put(node.nodeId(), node));
        Semaphore overall = new Semaphore(properties.gcMaxInflight());
        Map<String, Semaphore> perNode = new HashMap<>();
        nodes.keySet().forEach(id -> perNode.put(id, new Semaphore(properties.gcMaxPerNode())));
        Set<String> failed = java.util.concurrent.ConcurrentHashMap.newKeySet();
        int batch = properties.gcBatch();

        while (true) {
            Map<String, List<List<DeletionTask>>> round = new LinkedHashMap<>();
            for (String nodeId : nodes.keySet()) {
                if (failed.contains(nodeId)) {
                    continue;
                }
                List<DeletionTask> tasks = repository.deletionTasks(nodeId, batch * properties.gcMaxPerNode());
                if (tasks.isEmpty()) {
                    continue;
                }
                if (states.get(nodeId) != State.ALIVE) {
                    // Not attempted: its rows stay until it confirms, once ALIVE again.
                    log.atInfo().addKeyValue("node_id", nodeId).addKeyValue("state", states.get(nodeId))
                            .log("node not ALIVE; its deletions wait");
                    failed.add(nodeId);
                    continue;
                }
                List<List<DeletionTask>> batches = new ArrayList<>();
                for (int i = 0; i < tasks.size(); i += batch) {
                    batches.add(tasks.subList(i, Math.min(i + batch, tasks.size())));
                }
                round.put(nodeId, batches);
            }
            if (round.isEmpty()) {
                return d;
            }
            try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
                for (Map.Entry<String, List<List<DeletionTask>>> e : round.entrySet()) {
                    Node node = nodes.get(e.getKey());
                    Semaphore mine = perNode.get(e.getKey());
                    for (List<DeletionTask> tasks : e.getValue()) {
                        pool.submit(() -> {
                            mine.acquireUninterruptibly();
                            overall.acquireUninterruptibly();
                            try {
                                deleteOn(node, tasks, d, failed);
                            } catch (RuntimeException ex) {
                                failed.add(node.nodeId());
                                d.failures.incrementAndGet();
                                log.atError().addKeyValue("node_id", node.nodeId()).log("delete batch failed", ex);
                            } finally {
                                overall.release();
                                mine.release();
                            }
                        });
                    }
                }
            }
        }
    }

    /** One DeleteChunks call; confirms, in one transaction, exactly what the node itself reported. */
    private void deleteOn(Node node, List<DeletionTask> tasks, Deletes d, Set<String> failed) {
        NodeClient.DeleteAnswer answer = nodeClient.deleteChunks(node, tasks);
        List<UUID> ids = tasks.stream().map(DeletionTask::chunkId).toList();
        if (answer.response() == null) {
            failed.add(node.nodeId());
            d.failures.incrementAndGet();
            repository.recordDeletionFailure(node.nodeId(), ids, answer.error());
            log.atWarn().addKeyValue("node_id", node.nodeId()).addKeyValue("chunks", ids.size())
                    .log("deletions not confirmed; rows kept, retried next pass: {}", answer.error());
            return;
        }
        Set<UUID> asked = new HashSet<>(ids);
        Set<UUID> notGone = new HashSet<>();
        Integer confirmed = tx.execute(status -> {
            int n = 0;
            for (ChunkDeletionResult r : answer.response().getResultsList()) {
                UUID chunkId = UUID.fromString(r.getChunkId());
                if (!asked.remove(chunkId)) {
                    continue; // not asked for, or listed twice
                }
                boolean gone = r.getOutcome() == ChunkDeletionResult.Outcome.OUTCOME_DELETED
                        || r.getOutcome() == ChunkDeletionResult.Outcome.OUTCOME_ALREADY_ABSENT;
                if (!gone) {
                    notGone.add(chunkId);
                    continue;
                }
                if (repository.confirmDeleted(chunkId, node.nodeId())) {
                    n++;
                    (r.getOutcome() == ChunkDeletionResult.Outcome.OUTCOME_DELETED ? d.deleted : d.absent)
                            .incrementAndGet();
                    log.atInfo().addKeyValue("chunk_id", chunkId).addKeyValue("node_id", node.nodeId())
                            .addKeyValue("outcome", r.getOutcome()).log("chunk file deletion confirmed by its node");
                }
            }
            return n;
        });
        if (!asked.isEmpty()) {
            // The node didn't account for every chunk: keep those tasks.
            failed.add(node.nodeId());
            d.failures.incrementAndGet();
            repository.recordDeletionFailure(node.nodeId(), List.copyOf(asked), "no result for these chunks");
        }
        if (!notGone.isEmpty()) {
            // An outcome that says neither deleted nor absent: keep those tasks.
            failed.add(node.nodeId());
            d.failures.incrementAndGet();
            repository.recordDeletionFailure(node.nodeId(), List.copyOf(notGone), "outcome neither deleted nor absent");
            log.atWarn().addKeyValue("node_id", node.nodeId()).addKeyValue("chunks", notGone.size())
                    .log("deletions not confirmed: outcome neither deleted nor absent; rows kept");
        }
        if (confirmed == null || confirmed == 0) {
            // No progress: sending the same tasks again this pass would loop forever.
            failed.add(node.nodeId());
        }
    }

    /** Removes the metadata of retired objects whose files are all confirmed gone. */
    private int finish() {
        int n = 0;
        for (UUID id : repository.finalizeCandidates(properties.gcBatch())) {
            Boolean done = tx.execute(status -> {
                ObjectRow o = repository.lockObject(id).orElse(null);
                if (o == null || !o.status().retired() || repository.hasFilesLeft(id)) {
                    return false;
                }
                repository.removeObjectMetadata(id);
                return true;
            });
            if (Boolean.TRUE.equals(done)) {
                n++;
                log.atInfo().addKeyValue("object_id", id).log("object metadata removed: every file confirmed gone");
            }
        }
        return n;
    }

    @Override
    public void destroy() {
        if (scheduler != null) {
            scheduler.interrupt();
        }
    }
}
