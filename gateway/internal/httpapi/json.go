package httpapi

import (
	"encoding/hex"
	"strings"

	objectstorev1 "github.com/Ayomide1x/object-store/gen/go/objectstore/v1"
)

// The JSON shapes are written out here rather than generated from the
// protobuf messages, so the HTTP API stays stable if the wire format grows,
// and numbers stay numbers (protojson writes int64 as strings).

type putResponse struct {
	Key       string `json:"key"`
	ObjectID  string `json:"object_id"`
	SizeBytes int64  `json:"size_bytes"`
}

type rmResponse struct {
	Key      string `json:"key"`
	ObjectID string `json:"object_id"`
}

type objectSummary struct {
	Key                   string `json:"key"`
	ObjectID              string `json:"object_id"`
	SizeBytes             int64  `json:"size_bytes"`
	CommittedAtUnixMillis int64  `json:"committed_at_unix_millis"`
}

type listResponse struct {
	Objects []objectSummary `json:"objects"`
	// Empty when this page is the last; otherwise pass it as start_after.
	NextStartAfter string `json:"next_start_after"`
}

func toListResponse(r *objectstorev1.ListObjectsResponse) listResponse {
	out := listResponse{Objects: []objectSummary{}, NextStartAfter: r.GetNextStartAfter()}
	for _, o := range r.GetObjects() {
		out.Objects = append(out.Objects, objectSummary{Key: o.GetKey(), ObjectID: o.GetObjectId(),
			SizeBytes: o.GetSizeBytes(), CommittedAtUnixMillis: o.GetCommittedAtUnixMillis()})
	}
	return out
}

type replicaJSON struct {
	NodeID    string `json:"node_id"`
	State     string `json:"state"`
	Origin    string `json:"origin"`
	Liveness  string `json:"liveness"`
	BadReason string `json:"bad_reason,omitempty"`
}

type chunkJSON struct {
	ChunkID   string        `json:"chunk_id"`
	Index     int32         `json:"index"`
	SizeBytes int64         `json:"size_bytes"`
	SHA256    string        `json:"sha256"`
	Replicas  []replicaJSON `json:"replicas"`
	// By the reconciler's rule: what counts toward the 3-replica target.
	CountedReplicas int32 `json:"counted_replicas"`
}

type statResponse struct {
	Key                   string      `json:"key"`
	ObjectID              string      `json:"object_id"`
	SizeBytes             int64       `json:"size_bytes"`
	CommittedAtUnixMillis int64       `json:"committed_at_unix_millis"`
	Chunks                []chunkJSON `json:"chunks"`
}

func toStatResponse(key string, r *objectstorev1.StatObjectResponse) statResponse {
	out := statResponse{Key: key, ObjectID: r.GetObjectId(), SizeBytes: r.GetSizeBytes(),
		CommittedAtUnixMillis: r.GetCommittedAtUnixMillis(), Chunks: []chunkJSON{}}
	for _, c := range r.GetChunks() {
		cj := chunkJSON{ChunkID: c.GetChunkId(), Index: c.GetIndex(), SizeBytes: c.GetSizeBytes(),
			SHA256: hex.EncodeToString(c.GetSha256()), Replicas: []replicaJSON{}, CountedReplicas: c.GetCountedReplicas()}
		for _, rep := range c.GetReplicas() {
			cj.Replicas = append(cj.Replicas, replicaJSON{NodeID: rep.GetNodeId(),
				State: strings.TrimPrefix(rep.GetState().String(), "STATE_"), Origin: rep.GetOrigin(),
				Liveness: liveness(rep.GetLiveness()), BadReason: rep.GetBadReason()})
		}
		out.Chunks = append(out.Chunks, cj)
	}
	return out
}

func liveness(l objectstorev1.NodeLiveness) string {
	return strings.TrimPrefix(l.String(), "NODE_LIVENESS_")
}

type nodeJSON struct {
	NodeID                  string `json:"node_id"`
	Liveness                string `json:"liveness"`
	LivenessSinceUnixMillis int64  `json:"liveness_since_unix_millis"`
	LastHeartbeatUnixMillis int64  `json:"last_heartbeat_unix_millis"`
}

type repairPassJSON struct {
	PassID               int64  `json:"pass_id"`
	StartedAtUnixMillis  int64  `json:"started_at_unix_millis"`
	FinishedAtUnixMillis int64  `json:"finished_at_unix_millis"`
	SkippedReason        string `json:"skipped_reason,omitempty"`
	UnderReplicated      int32  `json:"under_replicated"`
	CopiesStarted        int32  `json:"copies_started"`
	ReplicasRecorded     int32  `json:"replicas_recorded"`
	FoundRecorded        int32  `json:"found_recorded"`
	BadFound             int32  `json:"bad_found"`
	Failures             int32  `json:"failures"`
}

type gcPassJSON struct {
	PassID               int64  `json:"pass_id"`
	StartedAtUnixMillis  int64  `json:"started_at_unix_millis"`
	FinishedAtUnixMillis int64  `json:"finished_at_unix_millis"`
	SkippedReason        string `json:"skipped_reason,omitempty"`
	Abandoned            int32  `json:"abandoned"`
	Superseded           int32  `json:"superseded"`
	TasksCreated         int32  `json:"tasks_created"`
	FilesDeleted         int32  `json:"files_deleted"`
	AlreadyAbsent        int32  `json:"already_absent"`
	Failures             int32  `json:"failures"`
	ObjectsRemoved       int32  `json:"objects_removed"`
}

type livenessEventJSON struct {
	EventID int64  `json:"event_id"`
	NodeID  string `json:"node_id"`
	// Empty for the events written when the coordinator started.
	From         string `json:"from"`
	To           string `json:"to"`
	AtUnixMillis int64  `json:"at_unix_millis"`
}

type clusterResponse struct {
	Nodes                 []nodeJSON `json:"nodes"`
	UnderReplicatedChunks int64      `json:"under_replicated_chunks"`
	PendingDeletions      int64      `json:"pending_deletions"`
	// When the two counts above were computed (the coordinator caches them briefly).
	CountsAsOfUnixMillis  int64               `json:"counts_as_of_unix_millis"`
	LastRepairPass        *repairPassJSON     `json:"last_repair_pass"`
	LastGcPass            *gcPassJSON         `json:"last_gc_pass"`
	RecentRepairPasses    []repairPassJSON    `json:"recent_repair_passes"`
	RecentGcPasses        []gcPassJSON        `json:"recent_gc_passes"`
	RecentLivenessEvents  []livenessEventJSON `json:"recent_liveness_events"`
	LivenessEventsDropped int64               `json:"liveness_events_dropped"`
}

func toRepairPass(p *objectstorev1.RepairPass) repairPassJSON {
	return repairPassJSON{PassID: p.GetPassId(), StartedAtUnixMillis: p.GetStartedAtUnixMillis(),
		FinishedAtUnixMillis: p.GetFinishedAtUnixMillis(), SkippedReason: p.GetSkippedReason(),
		UnderReplicated: p.GetUnderReplicated(), CopiesStarted: p.GetCopiesStarted(),
		ReplicasRecorded: p.GetReplicasRecorded(), FoundRecorded: p.GetFoundRecorded(),
		BadFound: p.GetBadFound(), Failures: p.GetFailures()}
}

func toGcPass(p *objectstorev1.GcPass) gcPassJSON {
	return gcPassJSON{PassID: p.GetPassId(), StartedAtUnixMillis: p.GetStartedAtUnixMillis(),
		FinishedAtUnixMillis: p.GetFinishedAtUnixMillis(), SkippedReason: p.GetSkippedReason(),
		Abandoned: p.GetAbandoned(), Superseded: p.GetSuperseded(), TasksCreated: p.GetTasksCreated(),
		FilesDeleted: p.GetFilesDeleted(), AlreadyAbsent: p.GetAlreadyAbsent(), Failures: p.GetFailures(),
		ObjectsRemoved: p.GetObjectsRemoved()}
}

func toClusterResponse(r *objectstorev1.GetClusterStatusResponse) clusterResponse {
	out := clusterResponse{Nodes: []nodeJSON{}, UnderReplicatedChunks: r.GetUnderReplicatedChunks(),
		PendingDeletions: r.GetPendingDeletions(), CountsAsOfUnixMillis: r.GetUnderReplicatedAsOfUnixMillis(),
		RecentRepairPasses: []repairPassJSON{}, RecentGcPasses: []gcPassJSON{},
		RecentLivenessEvents: []livenessEventJSON{}, LivenessEventsDropped: r.GetLivenessEventsDropped()}
	for _, n := range r.GetNodes() {
		out.Nodes = append(out.Nodes, nodeJSON{NodeID: n.GetNodeId(), Liveness: liveness(n.GetLiveness()),
			LivenessSinceUnixMillis: n.GetLivenessSinceUnixMillis(), LastHeartbeatUnixMillis: n.GetLastHeartbeatUnixMillis()})
	}
	if r.GetLastRepairPass() != nil {
		p := toRepairPass(r.GetLastRepairPass())
		out.LastRepairPass = &p
	}
	if r.GetLastGcPass() != nil {
		p := toGcPass(r.GetLastGcPass())
		out.LastGcPass = &p
	}
	for _, p := range r.GetRecentRepairPasses() {
		out.RecentRepairPasses = append(out.RecentRepairPasses, toRepairPass(p))
	}
	for _, p := range r.GetRecentGcPasses() {
		out.RecentGcPasses = append(out.RecentGcPasses, toGcPass(p))
	}
	for _, e := range r.GetRecentLivenessEvents() {
		from := ""
		if e.GetFrom() != objectstorev1.NodeLiveness_NODE_LIVENESS_UNSPECIFIED {
			from = liveness(e.GetFrom())
		}
		out.RecentLivenessEvents = append(out.RecentLivenessEvents, livenessEventJSON{EventID: e.GetEventId(),
			NodeID: e.GetNodeId(), From: from, To: liveness(e.GetTo()), AtUnixMillis: e.GetAtUnixMillis()})
	}
	return out
}
