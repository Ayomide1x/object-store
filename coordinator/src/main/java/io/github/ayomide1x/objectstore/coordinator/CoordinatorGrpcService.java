package io.github.ayomide1x.objectstore.coordinator;

import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;

import io.github.ayomide1x.objectstore.v1.ChunkInfo;
import io.github.ayomide1x.objectstore.v1.ChunkStatus;
import io.github.ayomide1x.objectstore.v1.DeleteObjectRequest;
import io.github.ayomide1x.objectstore.v1.DeleteObjectResponse;
import io.github.ayomide1x.objectstore.v1.ListObjectsRequest;
import io.github.ayomide1x.objectstore.v1.ListObjectsResponse;
import io.github.ayomide1x.objectstore.v1.ObjectSummary;
import io.github.ayomide1x.objectstore.v1.ReplicaStatus;
import io.github.ayomide1x.objectstore.v1.StatObjectRequest;
import io.github.ayomide1x.objectstore.v1.StatObjectResponse;
import io.github.ayomide1x.objectstore.v1.ChunkPlacement;
import io.github.ayomide1x.objectstore.v1.CompleteUploadRequest;
import io.github.ayomide1x.objectstore.v1.CompleteUploadResponse;
import io.github.ayomide1x.objectstore.v1.CoordinatorServiceGrpc;
import io.github.ayomide1x.objectstore.v1.GetObjectRequest;
import io.github.ayomide1x.objectstore.v1.GetObjectResponse;
import io.github.ayomide1x.objectstore.v1.InitiateUploadRequest;
import io.github.ayomide1x.objectstore.v1.InitiateUploadResponse;
import io.github.ayomide1x.objectstore.v1.HeartbeatRequest;
import io.github.ayomide1x.objectstore.v1.HeartbeatResponse;
import io.github.ayomide1x.objectstore.v1.NodeLiveness;
import io.github.ayomide1x.objectstore.v1.NodeLocation;
import io.github.ayomide1x.objectstore.v1.ReportBadReplicaRequest;
import io.github.ayomide1x.objectstore.v1.ReportBadReplicaResponse;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * gRPC adapter for {@link MetadataService}. Any {@code BindableService} bean is
 * registered with the gRPC server by Spring Boot.
 */
@Service
public class CoordinatorGrpcService extends CoordinatorServiceGrpc.CoordinatorServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(CoordinatorGrpcService.class);

    private final MetadataService metadata;
    private final ReplicaHealthService replicaHealth;
    private final LivenessService liveness;

    public CoordinatorGrpcService(MetadataService metadata, ReplicaHealthService replicaHealth,
            LivenessService liveness) {
        this.metadata = metadata;
        this.replicaHealth = replicaHealth;
        this.liveness = liveness;
    }

    @Override
    public void heartbeat(HeartbeatRequest request, StreamObserver<HeartbeatResponse> response) {
        respond(response, () -> {
            LivenessTracker.State state = liveness.heartbeat(request.getNodeId(), request.getInstanceId());
            return HeartbeatResponse.newBuilder().setLiveness(toProto(state)).build();
        });
    }

    @Override
    public void initiateUpload(InitiateUploadRequest request, StreamObserver<InitiateUploadResponse> response) {
        respond(response, () -> {
            MetadataService.UploadPlan plan = metadata.initiateUpload(request.getKey(), request.getSizeBytes());
            log.atInfo()
                    .addKeyValue("object_id", plan.objectId())
                    .addKeyValue("key", request.getKey())
                    .addKeyValue("size_bytes", request.getSizeBytes())
                    .addKeyValue("chunks", plan.chunks().size())
                    .log("upload initiated");
            InitiateUploadResponse.Builder b = InitiateUploadResponse.newBuilder()
                    .setObjectId(plan.objectId().toString());
            for (MetadataService.ChunkPlacement c : plan.chunks()) {
                b.addChunks(ChunkPlacement.newBuilder()
                        .setChunkId(c.chunkId().toString())
                        .setIndex(c.index())
                        .setSizeBytes(c.sizeBytes())
                        .addAllNodes(c.nodes().stream().map(this::clientLocation).toList()));
            }
            return b.build();
        });
    }

    @Override
    public void completeUpload(CompleteUploadRequest request, StreamObserver<CompleteUploadResponse> response) {
        respond(response, () -> {
            List<MetadataService.CompletedChunk> chunks = request.getChunksList().stream()
                    .map(c -> new MetadataService.CompletedChunk(
                            parseUuid("chunk_id", c.getChunkId()), c.getIndex(), c.getSizeBytes(),
                            c.getSha256().toByteArray()))
                    .toList();
            metadata.completeUpload(parseUuid("object_id", request.getObjectId()), request.getKey(),
                    request.getSizeBytes(), chunks);
            return CompleteUploadResponse.getDefaultInstance();
        });
    }

    @Override
    public void getObject(GetObjectRequest request, StreamObserver<GetObjectResponse> response) {
        respond(response, () -> {
            StoredObject obj = metadata.getObject(request.getKey())
                    .orElseThrow(() -> new RequestException(Status.Code.NOT_FOUND,
                            "key '%s' not found".formatted(request.getKey())));
            GetObjectResponse.Builder b = GetObjectResponse.newBuilder()
                    .setObjectId(obj.objectId().toString())
                    .setSizeBytes(obj.sizeBytes());
            for (StoredObject.Chunk c : obj.chunks()) {
                b.addChunks(ChunkInfo.newBuilder()
                        .setChunkId(c.chunkId().toString())
                        .setIndex(c.index())
                        .setSizeBytes(c.sizeBytes())
                        .setSha256(ByteString.copyFrom(c.sha256()))
                        .addAllReplicas(c.replicas().stream().map(this::clientLocation).toList()));
            }
            return b.build();
        });
    }

    @Override
    public void reportBadReplica(ReportBadReplicaRequest request, StreamObserver<ReportBadReplicaResponse> response) {
        respond(response, () -> {
            if (request.getNodeId().isEmpty()) {
                throw RequestException.invalid("node_id is required");
            }
            ReportBadReplicaResponse.Verdict verdict = replicaHealth.reportBadReplica(
                    parseUuid("chunk_id", request.getChunkId()), request.getNodeId(),
                    request.getReason().name(), request.getDetail());
            return ReportBadReplicaResponse.newBuilder().setVerdict(verdict).build();
        });
    }

    @Override
    public void deleteObject(DeleteObjectRequest request, StreamObserver<DeleteObjectResponse> response) {
        respond(response, () -> DeleteObjectResponse.newBuilder()
                .setObjectId(metadata.deleteObject(request.getKey()).toString()).build());
    }

    @Override
    public void listObjects(ListObjectsRequest request, StreamObserver<ListObjectsResponse> response) {
        respond(response, () -> {
            ListObjectsResponse.Builder b = ListObjectsResponse.newBuilder();
            for (MetadataRepository.ObjectSummary o : metadata.listObjects(request.getPrefix())) {
                b.addObjects(ObjectSummary.newBuilder().setKey(o.key()).setObjectId(o.objectId().toString())
                        .setSizeBytes(o.sizeBytes()).setCommittedAtUnixMillis(millis(o.committedAt())));
            }
            return b.build();
        });
    }

    @Override
    public void statObject(StatObjectRequest request, StreamObserver<StatObjectResponse> response) {
        respond(response, () -> {
            MetadataRepository.ObjectDetail obj = metadata.statObject(request.getKey())
                    .orElseThrow(() -> new RequestException(Status.Code.NOT_FOUND,
                            "key '%s' not found".formatted(request.getKey())));
            StatObjectResponse.Builder b = StatObjectResponse.newBuilder().setObjectId(obj.objectId().toString())
                    .setSizeBytes(obj.sizeBytes()).setCommittedAtUnixMillis(millis(obj.committedAt()));
            for (MetadataRepository.ChunkDetail c : obj.chunks()) {
                ChunkStatus.Builder cb = ChunkStatus.newBuilder().setChunkId(c.chunkId().toString())
                        .setIndex(c.index()).setSizeBytes(c.sizeBytes())
                        .setSha256(c.sha256() == null ? ByteString.EMPTY : ByteString.copyFrom(c.sha256()));
                for (MetadataRepository.ReplicaDetail r : c.replicas()) {
                    cb.addReplicas(ReplicaStatus.newBuilder().setNodeId(r.nodeId())
                            .setState(r.state().equals("BAD") ? ReplicaStatus.State.STATE_BAD : ReplicaStatus.State.STATE_HEALTHY)
                            .setOrigin(r.origin()).setBadReason(r.badReason() == null ? "" : r.badReason())
                            .setLiveness(toProto(liveness.stateOf(r.nodeId()))));
                }
                b.addChunks(cb);
            }
            return b.build();
        });
    }

    private static long millis(java.time.OffsetDateTime t) {
        return t == null ? 0 : t.toInstant().toEpochMilli();
    }

    /** Runs a unary handler and translates exceptions into gRPC statuses. */
    private static <T> void respond(StreamObserver<T> observer, Supplier<T> handler) {
        T result;
        try {
            result = handler.get();
        } catch (RequestException e) {
            observer.onError(Status.fromCode(e.code()).withDescription(e.getMessage()).asRuntimeException());
            return;
        } catch (RuntimeException e) {
            log.error("unhandled error", e);
            observer.onError(Status.INTERNAL.withDescription("internal error").asRuntimeException());
            return;
        }
        observer.onNext(result);
        observer.onCompleted();
    }

    private NodeLocation clientLocation(Node node) {
        return NodeLocation.newBuilder().setNodeId(node.nodeId()).setAddress(node.clientAddr())
                .setLiveness(toProto(liveness.stateOf(node.nodeId()))).build();
    }

    private static NodeLiveness toProto(LivenessTracker.State state) {
        return switch (state) {
            case ALIVE -> NodeLiveness.NODE_LIVENESS_ALIVE;
            case SUSPECT -> NodeLiveness.NODE_LIVENESS_SUSPECT;
            case DEAD -> NodeLiveness.NODE_LIVENESS_DEAD;
            case UNKNOWN -> NodeLiveness.NODE_LIVENESS_UNKNOWN;
        };
    }

    private static UUID parseUuid(String field, String value) {
        try {
            return UUID.fromString(value);
        } catch (IllegalArgumentException e) {
            throw RequestException.invalid("%s '%s' is not a UUID".formatted(field, value));
        }
    }
}
