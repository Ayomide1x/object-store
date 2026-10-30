package io.github.ayomide1x.objectstore.coordinator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import io.github.ayomide1x.objectstore.coordinator.FakeNodes.Held;
import io.github.ayomide1x.objectstore.coordinator.LivenessTrackerTest.FakeTicker;
import io.github.ayomide1x.objectstore.coordinator.MetadataService.ChunkPlacement;
import io.github.ayomide1x.objectstore.coordinator.MetadataService.CompletedChunk;
import io.github.ayomide1x.objectstore.coordinator.MetadataService.UploadPlan;
import io.github.ayomide1x.objectstore.v1.ChunkStat;
import io.github.ayomide1x.objectstore.v1.GetClusterStatusRequest;
import io.github.ayomide1x.objectstore.v1.GetClusterStatusResponse;
import io.github.ayomide1x.objectstore.v1.GetObjectRequest;
import io.github.ayomide1x.objectstore.v1.GetObjectResponse;
import io.github.ayomide1x.objectstore.v1.InitiateUploadRequest;
import io.github.ayomide1x.objectstore.v1.InitiateUploadResponse;
import io.github.ayomide1x.objectstore.v1.ListObjectsRequest;
import io.github.ayomide1x.objectstore.v1.ListObjectsResponse;
import io.github.ayomide1x.objectstore.v1.NodeAddressSet;
import io.github.ayomide1x.objectstore.v1.NodeLiveness;
import io.github.ayomide1x.objectstore.v1.NodeLocation;
import io.github.ayomide1x.objectstore.v1.NodeStatus;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * The RPCs and fields stage 10 added for the gateway: ListObjects pagination,
 * the node address set, and GetClusterStatus. Calls go through the gRPC
 * adapter itself (in process), so request defaults are exercised as a caller
 * sends them. Schedulers and the liveness sweeper are off.
 */
@Testcontainers
@SpringBootTest(properties = {
        "spring.grpc.server.port=0",
        "coordinator.node-rpc-timeout=500ms",
        "coordinator.copy-timeout=2s",
        "coordinator.liveness-sweep=0",
        "coordinator.repair-interval=0",
        "coordinator.gc-interval=0",
})
class GatewayRpcIT {

    static final List<String> ALL = List.of("node1", "node2", "node3", "node4");

    @Container
    @ServiceConnection
    static PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:18");

    static final FakeNodes nodes = new FakeNodes(4);
    static final FakeTicker clock = new FakeTicker();

    /** Registers the fakes with distinct addresses: internal 127.0.0.1:port, client localhost:port. */
    @DynamicPropertySource
    static void registerNodes(DynamicPropertyRegistry registry) {
        registry.add("coordinator.nodes", () -> Arrays.stream(nodes.nodesProperty().split(","))
                .map(e -> {
                    String[] p = e.split("\\|");
                    return p[0] + "|" + p[1] + "|" + p[2].replace("127.0.0.1", "localhost");
                })
                .collect(Collectors.joining(",")));
    }

    @TestConfiguration
    static class Clock {
        @Bean
        @Primary
        Ticker fakeTicker() {
            return clock;
        }
    }

    private final CoordinatorGrpcService grpc;
    private final MetadataService metadata;
    private final LivenessService liveness;
    private final RepairService repair;
    private final CoordinatorProperties properties;
    private final JdbcClient jdbc;

    @Autowired
    GatewayRpcIT(CoordinatorGrpcService grpc, MetadataService metadata, LivenessService liveness, RepairService repair,
            CoordinatorProperties properties, JdbcClient jdbc) {
        this.grpc = grpc;
        this.metadata = metadata;
        this.liveness = liveness;
        this.repair = repair;
        this.properties = properties;
        this.jdbc = jdbc;
    }

    @BeforeEach
    void allAlive() {
        jdbc.sql("TRUNCATE keys, pending_deletions, chunk_replicas, chunk_placements, chunks, objects, repair_passes, gc_passes").update();
        nodes.reset();
        beatAllBut(null);
        beatAllBut(null);
    }

    private void beatAllBut(String skipped) {
        ALL.stream().filter(n -> !n.equals(skipped)).forEach(n -> liveness.heartbeat(n, "test-instance"));
    }

    /** Calls a unary handler of the adapter in process and returns its response, or throws its status. */
    private interface Handler<Q, R> {
        void call(Q request, StreamObserver<R> observer);
    }

    private static <Q, R> R call(Handler<Q, R> handler, Q request) {
        AtomicReference<R> result = new AtomicReference<>();
        AtomicReference<Throwable> error = new AtomicReference<>();
        handler.call(request, new StreamObserver<>() {
            @Override
            public void onNext(R value) {
                result.set(value);
            }

            @Override
            public void onError(Throwable t) {
                error.set(t);
            }

            @Override
            public void onCompleted() {
            }
        });
        if (error.get() != null) {
            throw (RuntimeException) error.get();
        }
        return result.get();
    }

    private static byte[] sha(UUID chunkId) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(chunkId.toString().getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    /** Commits a 0-byte object: no chunks, so no node is asked anything. */
    private void commitEmpty(String key) {
        UploadPlan plan = metadata.initiateUpload(key, 0);
        metadata.completeUpload(plan.objectId(), key, 0, List.of());
    }

    /** A committed one-chunk object, with replicas on all 3 placed nodes. */
    private record One(UUID chunkId, List<String> placed) {
    }

    private One commitOneChunk(String key) {
        UploadPlan plan = metadata.initiateUpload(key, 10);
        ChunkPlacement c = plan.chunks().getFirst();
        List<String> placed = c.nodes().stream().map(Node::nodeId).toList();
        for (String n : placed) {
            nodes.get(n).held.put(c.chunkId().toString(), new Held(ChunkStat.State.STATE_FOUND, 10, sha(c.chunkId())));
        }
        metadata.completeUpload(plan.objectId(), key, 10, List.of(new CompletedChunk(c.chunkId(), 0, 10, sha(c.chunkId()))));
        return new One(c.chunkId(), placed);
    }

    private void markBad(UUID chunk, String node) {
        jdbc.sql("UPDATE chunk_replicas SET state = 'BAD', bad_reason = 'test', marked_bad_at = now() WHERE chunk_id = :c AND node_id = :n")
                .param("c", chunk).param("n", node).update();
    }

    /** Makes node DEAD, then lets extra time pass, keeping every other node ALIVE. */
    private void dead(String node, Duration extra) {
        clock.advance(Duration.ofSeconds(16));
        beatAllBut(node);
        liveness.sweep();
        assertThat(liveness.stateOf(node)).isEqualTo(LivenessTracker.State.DEAD);
        clock.advance(extra);
        beatAllBut(node);
        liveness.sweep();
    }

    private ListObjectsResponse list(String prefix, String startAfter, int limit) {
        return call(grpc::listObjects, ListObjectsRequest.newBuilder().setPrefix(prefix).setStartAfter(startAfter)
                .setLimit(limit).build());
    }

    /** Every key, page by page, as a caller pages: following next_start_after until it is empty. */
    private List<List<String>> pages(String prefix, int limit) {
        List<List<String>> pages = new ArrayList<>();
        String after = "";
        do {
            ListObjectsResponse r = list(prefix, after, limit);
            pages.add(r.getObjectsList().stream().map(o -> o.getKey()).toList());
            after = r.getNextStartAfter();
            assertThat(pages).as("pagination must end").hasSizeLessThan(1000);
        } while (!after.isEmpty());
        return pages;
    }

    private static List<String> keys(String prefix, int n) {
        return IntStream.range(0, n).mapToObj(i -> "%s%04d".formatted(prefix, i)).toList();
    }

    /** Mutation: next_start_after is the first key of the page instead of the last. */
    @Test
    void moreKeysThanTheLimitPageWithNoGapsOrDuplicates() {
        List<String> all = keys("k/", 50);
        all.forEach(this::commitEmpty);

        List<List<String>> pages = pages("", 7);

        assertThat(pages).hasSize(8); // 7 full pages and one of 1
        pages.subList(0, 7).forEach(p -> assertThat(p).hasSize(7));
        assertThat(pages.stream().flatMap(List::stream).toList()).containsExactlyElementsOf(all);
    }

    @Test
    void anExactMultipleOfTheLimitEndsWithoutAnEmptyPage() {
        List<String> all = keys("k/", 21);
        all.forEach(this::commitEmpty);

        List<List<String>> pages = pages("", 7);

        assertThat(pages).hasSize(3);
        assertThat(pages.stream().flatMap(List::stream).toList()).containsExactlyElementsOf(all);
    }

    @Test
    void prefixAndPaginationTogether() {
        List<String> wanted = keys("p/", 23);
        List<String> other = new ArrayList<>(keys("o/", 5));
        other.addAll(keys("q/", 5));
        other.add("p"); // sorts before "p/" and lacks the prefix
        wanted.forEach(this::commitEmpty);
        other.forEach(this::commitEmpty);

        List<List<String>> pages = pages("p/", 5);

        assertThat(pages.stream().flatMap(List::stream).toList()).containsExactlyElementsOf(wanted);
        // A cursor from outside the prefix still lists only the prefix.
        assertThat(list("p/", "a", 100).getObjectsList()).hasSize(23);
        assertThat(list("p/", "zzz", 100).getObjectsList()).isEmpty();
    }

    @Test
    void aRemovedCursorKeyLeavesNoGap() {
        List<String> all = keys("k/", 12);
        all.forEach(this::commitEmpty);

        ListObjectsResponse first = list("", "", 5);
        String cursor = first.getNextStartAfter();
        assertThat(cursor).isEqualTo("k/0004");
        metadata.deleteObject(cursor);
        ListObjectsResponse second = list("", cursor, 5);

        assertThat(second.getObjectsList()).extracting(o -> o.getKey()).containsExactlyElementsOf(all.subList(5, 10));
    }

    @Test
    void theLimitDefaultsTo1000AndLargerLimitsAreClampedNegativeRejected() {
        List<String> all = keys("k/", 1003);
        all.forEach(this::commitEmpty);

        ListObjectsResponse byDefault = list("", "", 0);
        ListObjectsResponse clamped = list("", "", 5000);

        assertThat(byDefault.getObjectsCount()).isEqualTo(1000);
        assertThat(byDefault.getNextStartAfter()).isEqualTo(all.get(999));
        assertThat(clamped.getObjectsCount()).isEqualTo(1000);
        assertThat(clamped.getNextStartAfter()).isEqualTo(all.get(999));
        assertThat(list("", clamped.getNextStartAfter(), 5000).getObjectsList()).extracting(o -> o.getKey())
                .containsExactlyElementsOf(all.subList(1000, 1003));
        assertThatThrownBy(() -> list("", "", -1)).isInstanceOfSatisfying(StatusRuntimeException.class,
                e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT));
    }

    @Test
    void anEmptyListingHasNoCursor() {
        ListObjectsResponse r = list("", "", 0);
        assertThat(r.getObjectsList()).isEmpty();
        assertThat(r.getNextStartAfter()).isEmpty();
    }

    private static Map<String, String> addresses(List<NodeLocation> locations) {
        return locations.stream().collect(Collectors.toMap(NodeLocation::getNodeId, NodeLocation::getAddress));
    }

    /**
     * Mutations: the address set is ignored; UNSPECIFIED is treated as
     * INTERNAL.
     */
    @Test
    void theCallerChoosesClientOrInternalAddresses() {
        One one = commitOneChunk("addr");
        for (NodeAddressSet set : NodeAddressSet.values()) {
            if (set == NodeAddressSet.UNRECOGNIZED) {
                continue;
            }
            String host = set == NodeAddressSet.NODE_ADDRESS_SET_INTERNAL ? "127.0.0.1:" : "localhost:";
            InitiateUploadResponse plan = call(grpc::initiateUpload,
                    InitiateUploadRequest.newBuilder().setKey("plan").setSizeBytes(10).setNodeAddresses(set).build());
            GetObjectResponse obj = call(grpc::getObject,
                    GetObjectRequest.newBuilder().setKey("addr").setNodeAddresses(set).build());

            assertThat(addresses(plan.getChunks(0).getNodesList()).values()).as(set.name())
                    .hasSize(3).allMatch(a -> a.startsWith(host));
            Map<String, String> replicas = addresses(obj.getChunks(0).getReplicasList());
            assertThat(replicas.keySet()).as(set.name()).containsExactlyInAnyOrderElementsOf(one.placed());
            assertThat(replicas.values()).as(set.name()).allMatch(a -> a.startsWith(host));
        }
    }

    private GetClusterStatusResponse status() {
        return call(grpc::getClusterStatus, GetClusterStatusRequest.getDefaultInstance());
    }

    /**
     * Mutation: the status counts HEALTHY rows and ignores liveness and the
     * repair grace. The count must equal what a reconciler pass finds, in
     * every state the rule distinguishes.
     */
    @Test
    void underReplicatedIsCountedExactlyAsTheReconcilerCounts() {
        One full = commitOneChunk("full");
        One bad = commitOneChunk("bad");
        commitOneChunk("extra"); // on the node that dies, or not: the expected set below decides
        commitOneChunk("superseded");
        commitOneChunk("superseded"); // the first version is no longer current
        markBad(bad.chunkId(), bad.placed().get(0));
        assertThat(status().getUnderReplicatedChunks()).as("only the BAD replica's chunk").isEqualTo(1);

        // A node DEAD within the grace still counts as holding its copies.
        String down = full.placed().get(0);
        dead(down, properties.repairGrace().minusSeconds(1));
        assertThat(status().getUnderReplicatedChunks()).as("DEAD within the grace").isEqualTo(1);

        // Past the grace, every current chunk with a HEALTHY row on it counts too.
        clock.advance(Duration.ofSeconds(2));
        beatAllBut(down);
        Set<UUID> expected = new HashSet<>(List.of(bad.chunkId()));
        jdbc.sql("""
                SELECT r.chunk_id FROM chunk_replicas r JOIN chunks c ON c.chunk_id = r.chunk_id
                JOIN keys k ON k.object_id = c.object_id WHERE r.node_id = :n AND r.state = 'HEALTHY'
                """).param("n", down).query(UUID.class).list().forEach(expected::add);
        assertThat(expected).contains(full.chunkId());
        long counted = status().getUnderReplicatedChunks();

        assertThat(counted).as("past the grace").isEqualTo(expected.size());
        assertThat(counted).as("equals the reconciler's own count").isEqualTo(repair.runPass().underReplicated());
    }

    @Test
    void nodesShowLivenessAndPersistedTimes() {
        dead("node2", Duration.ZERO);

        GetClusterStatusResponse st = status();

        assertThat(st.getNodesList()).extracting(NodeStatus::getNodeId).containsExactlyElementsOf(ALL);
        Map<String, NodeLiveness> states = st.getNodesList().stream()
                .collect(Collectors.toMap(NodeStatus::getNodeId, NodeStatus::getLiveness));
        assertThat(states).containsEntry("node2", NodeLiveness.NODE_LIVENESS_DEAD)
                .containsEntry("node1", NodeLiveness.NODE_LIVENESS_ALIVE);
        assertThat(st.getNodesList()).allMatch(n -> n.getLivenessSinceUnixMillis() > 0 && n.getLastHeartbeatUnixMillis() > 0);
    }

    @Test
    void pendingDeletionsAreCounted() {
        commitOneChunk("gone");
        metadata.deleteObject("gone");
        assertThat(status().getPendingDeletions()).isZero();
        jdbc.sql("""
                INSERT INTO pending_deletions (chunk_id, node_id, tombstone, reason)
                SELECT p.chunk_id, p.node_id, true, 'deleted' FROM chunk_placements p
                """).update();

        assertThat(status().getPendingDeletions()).isEqualTo(3);
    }

    @Test
    void recentPassesAreTheEventfulOnesNewestFirstAndTheLastPassIsAlwaysShown() {
        jdbc.sql("INSERT INTO repair_passes (finished_at, under_replicated) VALUES (now(), 2)").update();
        jdbc.sql("INSERT INTO repair_passes (finished_at, skipped_reason) VALUES (now(), 'nodes UNKNOWN: [node1]')").update();
        jdbc.sql("INSERT INTO repair_passes (finished_at) SELECT now() FROM generate_series(1, 20)").update(); // idle
        jdbc.sql("INSERT INTO gc_passes (finished_at, files_deleted) SELECT now(), 1 FROM generate_series(1, 15)").update();
        jdbc.sql("INSERT INTO gc_passes (finished_at) VALUES (now())").update(); // idle, and the newest

        GetClusterStatusResponse st = status();

        assertThat(st.getRecentRepairPassesList()).hasSize(2);
        assertThat(st.getRecentRepairPasses(0).getSkippedReason()).startsWith("nodes UNKNOWN");
        assertThat(st.getRecentRepairPasses(1).getUnderReplicated()).isEqualTo(2);
        assertThat(st.getLastRepairPass().getPassId()).isGreaterThan(st.getRecentRepairPasses(0).getPassId());
        assertThat(st.getRecentGcPassesList()).hasSize(ClusterStatusService.DEFAULT_RECENT_PASSES)
                .allMatch(p -> p.getFilesDeleted() == 1);
        assertThat(st.getRecentGcPassesList()).extracting(p -> p.getPassId()).isSortedAccordingTo((a, b) -> Long.compare(b, a));
        assertThat(st.getLastGcPass().getFilesDeleted()).isZero();

        GetClusterStatusResponse more = call(grpc::getClusterStatus,
                GetClusterStatusRequest.newBuilder().setRecentPasses(500).build());
        assertThat(more.getRecentGcPassesList()).hasSize(15);
        assertThatThrownBy(() -> call(grpc::getClusterStatus, GetClusterStatusRequest.newBuilder().setRecentPasses(-1).build()))
                .isInstanceOfSatisfying(StatusRuntimeException.class,
                        e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT));
    }

    @Test
    void noPassesYetLeavesTheLastPassesUnset() {
        GetClusterStatusResponse st = status();
        assertThat(st.hasLastRepairPass()).isFalse();
        assertThat(st.hasLastGcPass()).isFalse();
    }

    private Map<String, Long> rowCounts() {
        List<String> tables = List.of("nodes", "objects", "keys", "chunks", "chunk_placements", "chunk_replicas",
                "pending_deletions", "repair_passes", "gc_passes");
        return tables.stream().collect(Collectors.toMap(t -> t,
                t -> jdbc.sql("SELECT count(*) FROM " + t).query(Long.class).single()));
    }

    private String nodesSnapshot() {
        return jdbc.sql("SELECT string_agg(node_id || state || coalesce(state_changed_at::text, '') || coalesce(last_heartbeat_at::text, ''), ',' ORDER BY node_id) FROM nodes")
                .query(String.class).single();
    }

    /** Mutation: the status RPC prunes old pass rows (a write). */
    @Test
    void theStatusIsReadOnly() {
        commitOneChunk("r");
        // Pass rows far older than any retention: a pass would prune them.
        jdbc.sql("INSERT INTO repair_passes (started_at, finished_at) VALUES (now() - interval '400 days', now() - interval '400 days')").update();
        jdbc.sql("INSERT INTO gc_passes (started_at, finished_at) VALUES (now() - interval '400 days', now() - interval '400 days')").update();
        dead("node3", properties.repairGrace().plusSeconds(1)); // something to repair, if anything acted
        Map<String, Long> before = rowCounts();
        String nodesBefore = nodesSnapshot();

        for (int i = 0; i < 3; i++) {
            status();
        }

        assertThat(rowCounts()).isEqualTo(before);
        assertThat(nodesSnapshot()).isEqualTo(nodesBefore);
        assertThat(nodes.pushes).isEmpty();
    }
}
