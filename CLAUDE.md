# Object Store

A small S3-style distributed object store. Clients upload objects under a key. Objects are split into 4 MiB chunks, each chunk is replicated across three storage nodes, and each chunk is checksummed. A coordinator holds all metadata in PostgreSQL. It tracks node liveness through heartbeats and re-replicates chunks that fall below their replication target.

The core design rule: **metadata plane and data plane are separate.** The coordinator decides *where* bytes go and records *what* exists. It never carries object bytes. Clients and storage nodes move the bytes between themselves.

## Stack

| Component | Language | Role |
|---|---|---|
| `coordinator/` | Java 25, Spring Boot, grpc-java | Metadata, placement, liveness, re-replication. PostgreSQL via Flyway-managed schema. |
| `node/` | Go | Stores chunk files on local disk, serves reads, replicates to peers, heartbeats to coordinator. |
| `client/` | Go | CLI: `put`, `get`, `ls`, `rm`, `stat`. |
| `proto/` | Protocol Buffers | Single schema. Generates Go code (node, client) and Java code (coordinator). |
| Infra | Docker Compose, PostgreSQL | One coordinator, four nodes (from stage 4), one Postgres. |

### gRPC surface (sketch — refined stage by stage, recorded in DECISIONS.md)

- `CoordinatorService`: initiate upload / allocate chunk placement, complete upload, get object (chunk list + replica locations), delete, list, heartbeat, report bad replica.
- `StorageNodeService`:
  - `WriteChunk`: **client-streaming**. The chunk arrives as a sequence of frames.
  - `ReadChunk`: **server-streaming**.
  - Replicate (node-to-node copy).
  - Delete.

Both languages are clients of each other: the coordinator calls nodes (replication orders, deletes), and nodes call the coordinator (heartbeats, reports).

## Repo layout

```
proto/objectstore/v1/   *.proto (the only source of truth for the wire format)
coordinator/            Gradle project; generated Java from proto/
go.mod                  single Go module (github.com/Ayomide1x/object-store)
gen/go/                 generated Go code from proto/ (committed)
node/                   storage node; cmd/node, internal/...
client/                 CLI; cmd/osctl
scripts/                demo and chaos scripts
docker-compose.yml
buf.yaml
buf.gen.yaml
Makefile
DECISIONS.md
```

## Conventions

**Go**
- `gofmt` and `go vet` must be clean.
- Wrap errors with context (`fmt.Errorf("write chunk %s: %w", id, err)`).
- Pass `context.Context` through every call that does I/O or RPC.
- No package-level mutable state.
- Table-driven tests with the standard `testing` package.

**Java**
- Constructor injection only.
- Every schema change is a Flyway migration. Never edit an applied migration.
- Metadata state transitions happen inside a single database transaction.
- Coordinator integration tests use Testcontainers against a real Postgres.

**Protobuf**
- Package `objectstore.v1`.
- Never renumber or reuse a field number. Removed fields get `reserved` (number and name).
- New fields must be safe when absent (proto3 default values).

**gRPC framing**
- Chunks are streamed as frames of ≤ 1 MiB. Never send a whole 4 MiB chunk as one message, because gRPC's default max message size is 4 MiB and protobuf overhead pushes a full chunk over it.
- Raising the limit is not the fix.

**Logging**
- Structured logs.
- Every line touching data includes `object_id`, `chunk_id`, and `node_id` where applicable.

**Config**
- Environment variables only, documented in `docker-compose.yml`.

**Ports**

| Service | Port |
|---|---|
| Coordinator gRPC | 9090 |
| Nodes | 9101–9104 |
| Postgres | 5432 |

Fixed ports, so stale processes are easy to spot.

## DECISIONS.md

Every non-obvious choice gets an entry, written during the stage it was made, not afterward. Format:

```
## <short title> (stage N)
**Chose:** ...
**Over:** <the alternatives actually considered>
**Because:** ...
**Breaks if:** <the condition under which this choice becomes wrong>
```

If a later stage reverses an earlier decision, add a new entry that references the old one. Never edit the old entry.

## Build order

Strict. One stage at a time. A stage is done only when its acceptance check passes on a clean `docker compose down -v && docker compose up --build`.

**1. Vertical slice.**
- Scope: proto with the minimum fields; Postgres with Flyway; coordinator; one node; CLI.
- Flow: `osctl put` of a file ≤ 4 MiB stores it as a single chunk on the single node, records it in Postgres, and `osctl get` returns it.
- No replication, no checksums, no heartbeats.
- *Accept:* `cmp` original and downloaded file → identical.

**2. Chunking and the commit protocol.**
- Objects of any size are split into 4 MiB chunks, each streamed in ≤ 1 MiB frames.
- Objects have a lifecycle (`PENDING` → `COMMITTED`). A key resolves only to a committed object. Overwriting a key swaps the pointer atomically.
- *Accept:*
  - Round-trip a 0-byte file, a 1-byte file, an exactly-4 MiB file, and a 100 MiB file.
  - A `get` issued during an in-flight overwrite returns either the old or the new object, never a mix.

**3. Integrity.**
- The client computes SHA-256 per chunk and sends it with the write.
- The node verifies the checksum before acknowledging, then writes the chunk durably: temp file → fsync → rename → fsync directory.
- Reads verify the checksum before returning data.
- *Accept:*
  - Flipping a byte in a chunk file on disk causes `get` to fail loudly, not return bad data.
  - Killing a node mid-write leaves no partial chunk file under its final name.

**4. Replication.**
- Compose now runs four nodes; each chunk goes to three of them. With exactly three nodes, a dead node leaves nowhere to re-replicate to.
- The coordinator chooses placement.
- A write is acknowledged once the chunk is durable and verified on at least 2 of 3 replicas.
- The write mechanism (client fan-out vs. primary-forwards-to-replicas) is decided at the start of this stage.
- Metadata records only replicas that actually acknowledged.
- *Accept:*
  - With all nodes up, every chunk has 3 recorded replicas.
  - With one node stopped, uploads still succeed with 2.
  - With two stopped, uploads fail cleanly and nothing becomes `COMMITTED`.

**5. Read path with failover.**
- `get` reads each chunk from one replica and falls back to the next on error or checksum mismatch.
- A checksum mismatch is reported to the coordinator, which marks that replica bad.
- *Accept:* corrupt one replica of one chunk → `get` still succeeds, and the bad replica is marked in Postgres.

**6. Heartbeats and liveness.**
- Nodes heartbeat on an interval.
- The coordinator moves nodes through `ALIVE` → `SUSPECT` → `DEAD` on missed beats.
- Placement excludes non-`ALIVE` nodes.
- *Accept:* `docker compose stop node2` → node2 reaches `DEAD` within the configured window. `start` → it returns to `ALIVE`.

**7. Re-replication.**
- A reconciler finds chunks with fewer than 3 healthy replicas and orders a node-to-node copy from a verified source to a new target.
- Concurrency is bounded. The reconciler is idempotent and safe to run twice.
- Over-replication (a "dead" node comes back) is handled deliberately and recorded in DECISIONS.md.
- *Accept:* stop a node → every chunk it held returns to 3 healthy replicas on the remaining nodes → restart it → replica counts settle without data loss.

**8. Deletes and garbage collection.**
- `rm` removes metadata first, then chunks are deleted asynchronously.
- Abandoned `PENDING` uploads older than a threshold are garbage-collected, both metadata and chunk files.
- *Accept:* kill the client mid-upload → after the GC window, no orphan chunk files remain on any node.

**9. Demo and deploy-readiness.**
- `scripts/demo.sh`:
  1. Upload a file.
  2. Kill a node.
  3. Show re-replication in progress.
  4. Corrupt a replica.
  5. Download successfully.
  6. Show final replica state.
- All images build for `linux/arm64`.
- *Accept:* the script runs end to end from a clean state.

## Invariants

These must hold at every stage after the one that introduces them. Code that would violate one is wrong even if tests pass.

1. **No partial reads.** A key resolves only to a `COMMITTED` object. Readers never observe a partially uploaded or partially overwritten object.
2. **Commit implies durability.** An object becomes `COMMITTED` only if every one of its chunks has ≥ 2 replicas that acknowledged a checksum-verified, fsynced write.
3. **Metadata never overstates.** A replica row exists only for a replica that acknowledged. It is better to under-count, and re-replicate unnecessarily, than to over-count and lose data.
4. **Chunks are immutable.** Once written under its final name, a chunk file is never modified in place. Chunk IDs are coordinator-assigned and never reused.
5. **Every byte served is verified.** No code path returns chunk data to a client or a peer without checking its SHA-256.
6. **Coordinator carries no object bytes.** Re-replication is node-to-node, ordered by the coordinator.
7. **Postgres is the only metadata truth.** Coordinator in-memory state is limited to what can be rebuilt from Postgres plus incoming heartbeats. Restarting the coordinator loses nothing.
8. **Re-replication is idempotent and bounded.** Running the reconciler twice causes no harm, and it never saturates the cluster.
9. **Wire compatibility.** Field numbers are never reused or renumbered.
10. **arm64-clean.** Every image builds and runs on `linux/arm64`. No dependency without an arm64 build.

## Out of scope

Do not build these. If a stage seems to need one, stop and say so.

- Coordinator high availability, leader election, or consensus (Raft/Paxos). The coordinator is a known single point of failure.
- S3 HTTP API compatibility, presigned URLs, bucket policies.
- Authentication, authorization, encryption at rest or in transit.
- Erasure coding.
- Content-addressed deduplication.
- Compression.
- Object versioning beyond atomic overwrite.
- Range reads.
- Resumable uploads.
- Rack/zone-aware placement.
- Rebalancing when a node is added.
- Block reports and background scrubbing. Silent disk loss is detected only at read time.
- Fencing tokens for stale nodes.
- Multi-region.

## Verification notes

- Before calling a stage passed, check the fixed ports for stale processes (`lsof -i :9090` etc.).
- Failures that look environmental must be reproduced on a clean `docker compose down -v` before being dismissed as environmental.
