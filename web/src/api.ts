// The gateway's JSON shapes (gateway/internal/httpapi/json.go) and a small
// fetch wrapper. Everything here is same-origin.

export type Liveness = "ALIVE" | "SUSPECT" | "DEAD" | "UNKNOWN";

export interface NodeStatus {
  node_id: string;
  liveness: Liveness;
  liveness_since_unix_millis: number;
  last_heartbeat_unix_millis: number;
}

export interface RepairPass {
  pass_id: number;
  started_at_unix_millis: number;
  finished_at_unix_millis: number;
  skipped_reason?: string;
  under_replicated: number;
  copies_started: number;
  replicas_recorded: number;
  found_recorded: number;
  bad_found: number;
  failures: number;
}

export interface GcPass {
  pass_id: number;
  started_at_unix_millis: number;
  finished_at_unix_millis: number;
  skipped_reason?: string;
  abandoned: number;
  superseded: number;
  tasks_created: number;
  files_deleted: number;
  already_absent: number;
  failures: number;
  objects_removed: number;
}

export interface LivenessEvent {
  event_id: number;
  node_id: string;
  /** Empty for the events written when the coordinator started. */
  from: Liveness | "";
  to: Liveness;
  at_unix_millis: number;
}

export interface Cluster {
  nodes: NodeStatus[];
  under_replicated_chunks: number;
  pending_deletions: number;
  counts_as_of_unix_millis: number;
  last_repair_pass: RepairPass | null;
  last_gc_pass: GcPass | null;
  recent_repair_passes: RepairPass[];
  recent_gc_passes: GcPass[];
  recent_liveness_events: LivenessEvent[];
  liveness_events_dropped: number;
}

export interface ObjectSummary {
  key: string;
  object_id: string;
  size_bytes: number;
  committed_at_unix_millis: number;
}

export interface ObjectList {
  objects: ObjectSummary[];
  next_start_after: string;
}

export type ReplicaState = "HEALTHY" | "BAD";
export type ReplicaOrigin = "upload" | "repair" | "repair-found";

export interface Replica {
  node_id: string;
  state: ReplicaState;
  origin: ReplicaOrigin;
  liveness: Liveness;
  bad_reason?: string;
}

export interface Chunk {
  chunk_id: string;
  index: number;
  size_bytes: number;
  sha256: string;
  replicas: Replica[];
  counted_replicas: number;
}

export interface Stat {
  key: string;
  object_id: string;
  size_bytes: number;
  committed_at_unix_millis: number;
  chunks: Chunk[];
}

/** A non-2xx answer from the gateway, or no answer at all (status 0). */
export class ApiError extends Error {
  readonly status: number;
  readonly code: string;

  constructor(status: number, code: string, message: string) {
    super(message);
    this.status = status;
    this.code = code;
  }
}

export async function getJSON<T>(path: string, signal?: AbortSignal): Promise<T> {
  let resp: Response;
  try {
    resp = await fetch(path, { signal, headers: { Accept: "application/json" } });
  } catch (e) {
    if (signal?.aborted) {
      throw e;
    }
    throw new ApiError(0, "UNREACHABLE", "The gateway didn't answer.");
  }
  if (!resp.ok) {
    throw await errorFrom(resp);
  }
  return (await resp.json()) as T;
}

export async function deleteObject(path: string): Promise<void> {
  let resp: Response;
  try {
    resp = await fetch(path, { method: "DELETE" });
  } catch {
    throw new ApiError(0, "UNREACHABLE", "The gateway didn't answer.");
  }
  if (!resp.ok) {
    throw await errorFrom(resp);
  }
}

async function errorFrom(resp: Response): Promise<ApiError> {
  try {
    const body = (await resp.json()) as { error?: string; code?: string };
    return new ApiError(resp.status, body.code ?? "", body.error ?? resp.statusText);
  } catch {
    return new ApiError(resp.status, "", resp.statusText || `HTTP ${resp.status}`);
  }
}
