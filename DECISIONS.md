# Decisions

## Host gRPC with Spring Boot's built-in gRPC server (stage 1)
**Chose:** Spring Boot 4.1.2 `spring-boot-starter-grpc-server` (Netty, port 9090). Any `BindableService` bean is registered. No web stack (`web-application-type: none`). Java 25, Gradle 9.8.2 wrapper.
**Over:** a hand-rolled `ServerBuilder` in a `SmartLifecycle` bean; community starters (net.devh, LogNet); gRPC in servlet mode on Tomcat.
**Because:** Boot now ships official gRPC support with lifecycle, graceful shutdown, health and reflection. Hand-rolling reimplements those. The community starters are superseded and their Boot 4 support lags. Servlet mode needs HTTP/2 servlet config and a web stack we don't use. Boot 4.1.2 supports Java 17 through 27, and Gradle supports Java 25 from 9.1.0.
**Breaks if:** we need Netty or server tuning the auto-configuration doesn't expose, or Boot's gRPC module lags grpc-java on a fix we need.

## Proto code generation: buf for Go (committed), Gradle plugin for Java (build time) (stage 1)
**Chose:** `buf generate` with `protoc-gen-go` and `protoc-gen-go-grpc` pinned as `tool` directives in go.mod, writing to `gen/go/` (committed). The Java build uses the `com.google.protobuf` Gradle plugin with `../proto` as a source dir; that code is generated each build and not committed. `buf lint` and `buf breaking` run against main.
**Over:** plain `protoc` for Go; buf for Java too; generating Go code only inside Docker; copying generated code into each consumer.
**Because:** committed Go code means `go build` works without buf, which is the Go norm. `buf breaking` mechanically enforces invariant 9 (no renumbered or reused fields). The Spring Boot Gradle plugin aligns the `protoc` and `protoc-gen-grpc-java` versions with the protobuf and gRPC runtime on the classpath, which removes the main version-skew risk on the Java side.
**Breaks if:** someone edits a .proto without running `make proto`, so the committed Go code drifts from the schema. Generation is not yet checked in CI.

## Single Go module at the repo root (stage 1)
**Chose:** one module, `github.com/Ayomide1x/object-store`, at the repo root. `node/`, `client/` and `gen/go/` are packages in it. Node internals live under `node/internal/`, so the client cannot import them.
**Over:** three modules (`node`, `client`, `gen/go`) joined with `replace` directives and a `go.work`. CLAUDE.md's layout originally described `node/` and `client/` as separate modules.
**Because:** node and client always ship together from one repo, so module boundaries buy nothing except replace and workspace plumbing in every build, including both Dockerfiles. One go.mod means one dependency graph and one `go test ./...`.
**Breaks if:** node and client need independent versions or release cycles, or a consumer outside this repo needs to import the client without pulling in node dependencies.

## Chunk streams framed at ≤ 1 MiB, header frame first (stage 1)
**Chose:** `WriteChunk` takes a `oneof frame { header, data }`. The first message must be the header (`chunk_id`, `object_id`, `size_bytes`), and every later message is data of at most 1 MiB. `ReadChunk` responses are also at most 1 MiB each.
**Over:** `chunk_id` in gRPC metadata; repeating `chunk_id` in every frame; one message per chunk.
**Because:** the IDs are typed and in the schema, and the node checks them once. A full 4 MiB chunk plus protobuf overhead exceeds gRPC's default 4 MiB message limit in both directions, and raising the limit is not the fix.
**Breaks if:** per-frame overhead matters at much higher throughput, where larger frames would need a deliberate limit change.

## Node rejects malformed writes and never overwrites a chunk (stage 1)
**Chose:** the node creates chunk files with `O_CREATE|O_EXCL`; an existing chunk returns `ALREADY_EXISTS`. The RPC fails with `INVALID_ARGUMENT` if:
- the header is missing, repeated, or not first;
- a frame is empty or larger than 1 MiB;
- `size_bytes` is outside [0, 4 MiB];
- the bytes received differ from `size_bytes`.

On failure the node removes the partial file. Chunk IDs must be canonical lowercase UUIDs, so a chunk ID cannot escape the data directory.
**Over:** truncate-and-overwrite on an existing ID; trusting the stream to end at the right length; waiting until stage 3 to validate.
**Because:** invariant 4 (chunks are immutable) should hold from the first byte written, and a size mismatch is the only integrity signal stage 1 has.
**Breaks if:** the node crashes mid-write. The partial file then stays under its final name, because removal on error doesn't run after a crash. Stage 3's temp file, fsync and rename fixes this. Until then a crash can leave a truncated file, which stage 1 reads would serve.

## InitiateUpload persists nothing; metadata is written at CompleteUpload (stage 1)
**Chose:** `InitiateUpload` only generates IDs and picks a node. `CompleteUpload` inserts the objects, chunks and chunk_replicas rows in one `@Transactional` method, using the node acknowledgements the client reports.
**Over:** inserting the object row at initiate.
**Because:** without a lifecycle status, a row inserted at initiate would be readable before its bytes exist. The status column that would prevent that is stage 2's `PENDING` → `COMMITTED` lifecycle. This is the minimum that keeps a key from resolving to an incomplete object.
**Breaks if:** clients are untrusted. The coordinator records whatever chunk IDs and acknowledgements the client sends, and has no record of uploads that never complete, so their chunk files are orphaned. Stage 2 (lifecycle) and stage 8 (GC) are expected to replace this.

## Duplicate keys rejected by the UNIQUE constraint, not an existence check (stage 1)
**Chose:** `objects.key` has a `UNIQUE` constraint (`objects_key_unique`). `CompleteUpload` simply inserts; a unique violation on that constraint maps to `ALREADY_EXISTS`, and the whole transaction rolls back. `InitiateUpload` does not check whether the key exists.
**Over:** checking for the key at `InitiateUpload` or before the insert. That is read-then-write: two concurrent puts both pass the check. Also rejected: upsert, which is stage 2's atomic overwrite and would break "never a mix" without it.
**Because:** the database is the only place the check and the write are atomic. Verified: 5 concurrent puts of one key produced 1 success and 4 `ALREADY_EXISTS`, with no partial metadata.
**Breaks if:** stage 2 introduces overwrite, which needs old and new objects to coexist under one key. That needs a new entry and a migration replacing this constraint. Also, the losers' chunk bytes are already on the node when they are rejected; they are orphans until stage 8 GC.

## CompleteUpload atomicity is proven by the last-insert-failure test (stage 1)
**Chose:** `MetadataServiceCompleteUploadIT.failureOnLastInsertRollsBackEarlierInserts` is the test that proves `completeUpload` is one transaction. It acknowledges a node ID that isn't in `nodes`: the `objects` and `chunks` inserts succeed, the `chunk_replicas` insert violates the foreign key, and the test asserts zero rows remain in all three tables. This supplements "Duplicate keys rejected by the UNIQUE constraint" above.
**Over:** relying on the duplicate-key tests (second complete, concurrent completes) as evidence of atomicity.
**Because:** a duplicate key fails on the *first* insert, before there is anything to roll back. A mutation check showed this: with `@Transactional` removed, the three duplicate-key and success tests still passed, and only this test failed (`expected: 0L but was: 1L`).
**Breaks if:** the insert order changes so the failing insert is no longer last, or the foreign key from `chunk_replicas` to `nodes` is dropped. Either way this test silently stops covering atomicity. The test must always fail on the final insert.

## Coordinator-generated UUIDv4 for object and chunk IDs (stage 1)
**Chose:** `UUID.randomUUID()` in the coordinator, stored as Postgres `uuid`.
**Over:** `bigserial` (needs a database round-trip at initiate, which stage 1 avoids); UUIDv7 (Java has no built-in generator); client-generated IDs (violates "coordinator-assigned").
**Because:** random 122-bit IDs are never reused in practice (invariant 4), and they don't need persistence at allocation time.
**Breaks if:** index locality on very large chunk tables matters; random UUIDs scatter B-tree inserts.

## Separate internal and client-facing node addresses (stage 1)
**Chose:** each node has `internal_addr` (`node1:9101`, for containers) and `client_addr` (`127.0.0.1:9101`, returned to osctl). The coordinator returns `client_addr` to clients; coordinator-to-node and node-to-node calls in later stages use `internal_addr`. Each node listens on the same port inside and outside its container.
**Over:** `/etc/hosts` entries on the host; running osctl inside the Compose network; `network_mode: host` (limited on Docker Desktop for Mac); a node-address map on the client side.
**Because:** this is the advertised-address pattern (Kafka's `advertised.listeners`). Nothing changes on the host machine, and the coordinator stays the single source of addresses.
**Breaks if:** clients sit on several networks that each need a different address. One `client_addr` per node then isn't enough.

## Node registry seeded from COORDINATOR_NODES at startup (stage 1)
**Chose:** at startup, before the gRPC server listens, the coordinator upserts `nodes` rows from `COORDINATOR_NODES` (`node_id|internal_addr|client_addr`, comma-separated).
**Over:** a Flyway seed migration; node self-registration.
**Because:** deployment addresses don't belong in an immutable migration. Self-registration is heartbeat territory (stage 6).
**Breaks if:** the env var and the running nodes disagree. A node removed from the env var stays in Postgres and can still be chosen. Stage 6 replaces this.

## Plain SQL through JdbcClient (stage 1)
**Chose:** Spring `JdbcClient` with explicit SQL, with transaction boundaries in the service layer (`@Transactional`, `TransactionTemplate`).
**Over:** JPA/Hibernate; jOOQ.
**Because:** later stages need `SELECT … FOR UPDATE`, atomic pointer swaps and idempotent reconciler queries, and those are clearer as visible SQL than behind ORM dirty checking. jOOQ adds a codegen step that needs a live schema at build time.
**Breaks if:** the query count grows enough that hand-mapping rows becomes the main source of bugs.

## Flyway runs inside the coordinator at startup (stage 1)
**Chose:** Boot's Flyway auto-configuration migrates before any repository bean is used.
**Over:** a separate Flyway container in Compose.
**Because:** there's one coordinator, so there's no race between instances, and it avoids an extra service and ordering dependency.
**Breaks if:** there are several coordinators (out of scope), or a migration is slow enough to delay startup noticeably.

## All published ports bound to 127.0.0.1 (stage 1)
**Chose:** every Compose port mapping is `127.0.0.1:PORT:PORT`.
**Over:** the default `0.0.0.0`.
**Because:** there's no authentication (out of scope), so services must not be reachable from the LAN.
**Breaks if:** clients on other machines need access.

## Object lifecycle: plan persisted at initiate, PENDING → COMMITTED (stage 2)
**Supersedes:** "InitiateUpload persists nothing; metadata is written at CompleteUpload" (stage 1).
**Chose:** `InitiateUpload` inserts, in one transaction, an `objects` row with `status = 'PENDING'`, every `chunks` row (index and expected size), and a `chunk_placements` row per (chunk, target node). `CompleteUpload` then, in one transaction:
1. locks the object row (`FOR UPDATE`);
2. validates the request against that plan;
3. inserts `chunk_replicas` for the acknowledged nodes;
4. sets `status = 'COMMITTED'`;
5. swaps the key pointer.

A PENDING object is unreachable from any key.
**Over:** a stateless initiate validated with a signed token that carries the plan; a separate `uploads` table, with `objects` rows created only at commit.
**Because:** the coordinator needs a record of what it issued to validate against, and stage 8 GC needs a record of abandoned uploads. A status column on `objects` is the lifecycle CLAUDE.md describes, with no copy step between tables.
**Breaks if:** abandoned PENDING uploads accumulate without bound. Their metadata and chunk files stay until stage 8 GC.

## Key pointer table with last-commit-wins atomic swap (stage 2)
**Supersedes:** "Duplicate keys rejected by the UNIQUE constraint, not an existence check" (stage 1).
**Chose:** V2 drops `objects_key_unique` and adds `keys (key PRIMARY KEY, object_id)`. The last statement of `CompleteUpload` is `INSERT INTO keys … ON CONFLICT (key) DO UPDATE SET object_id = EXCLUDED.object_id`. A put to an existing key is an overwrite, not an error.
- Concurrent swaps of one key serialize on the `keys` row lock, or, for a new key, on its primary-key index entry.
- The transaction that commits last wins.
- Lock order is always own `objects` row, then `keys` row, so there are no deadlocks.

`GetObject` resolves `keys → objects` and LEFT JOINs to chunks and replicas in **one statement**, so a reader gets the complete chunk list of exactly one object. Chunk sets are fixed at initiate and chunk files are immutable, so a mix of old and new chunks cannot be expressed.
**Over:** winning by initiate order (a conditional swap: a slow older upload would get a success response but never become visible); SERIALIZABLE isolation (needs retry loops for no added guarantee); an advisory lock on a hash of the key (redundant with the row lock; collisions serialize unrelated keys); read-then-write (`SELECT`, then `UPDATE` or `INSERT`), which races on a new key.
**Breaks if:** clients need a deterministic winner that isn't "last to commit," for example conditional puts. That would need a version column and compare-and-swap.

## committed_at and keys.updated_at do not define version order (stage 2)
**Chose:** both columns are set with `clock_timestamp()` (wall-clock time at execution), not `now()` (transaction start time). They are informational only. **The `keys` row alone defines which version of a key is current.**
**Over:** `now()`; ordering versions by either timestamp.
**Because:** with `now()`, a winner whose transaction started earlier can show an earlier timestamp than the object it replaced. `clock_timestamp()` is closer to the truth, but `committed_at` is still set before the transaction blocks on the `keys` row lock. So under concurrent overwrites, the winner can still have an earlier `committed_at` than an object it replaced. Wall clocks can also step.
**Breaks if:** someone uses either column to answer "which version is newest." Anything that needs version order must read `keys`, or a future explicit version column.

## A key can only point to a COMMITTED object, enforced by a composite foreign key (stage 2)
**Chose:** `keys (object_id, object_status)` references `objects (object_id, status)`, where `keys.object_status` is constrained to `'COMMITTED'`, backed by `UNIQUE (object_id, status)` on `objects`.
**Over:** enforcing invariant 1 only in application code.
**Because:** Postgres rejects a pointer to a PENDING object regardless of the code path, and it rejects moving a referenced object back out of COMMITTED.
**Breaks if:** a later stage adds an object status that must stay readable (for example a `DELETING` state during stage 8). The FK target would have to change.

## All chunks allocated up front at initiate; 5 GiB object cap (stage 2)
**Chose:** `InitiateUpload` returns all n = ⌈size / 4 MiB⌉ placements. Objects over 5 GiB (1,280 chunks, ~115 KB response) are rejected with `INVALID_ARGUMENT`.
**Over:** one `AllocateChunk` RPC per chunk; streaming the plan.
**Because:** for 100 MiB (25 chunks), up front costs 2 coordinator RPCs and 2 transactions, versus 27 for per-chunk allocation. The latency difference (~25 round trips) is negligible next to the transfer. The deciding factor is that `CompleteUpload` gets a fixed, complete plan to validate against. `osctl` always knows the size from `stat`. The cap keeps the response far below the 4 MiB gRPC message limit (reached around ~45k chunks). 5 GiB matches S3's single-PUT limit.
**Breaks if:** uploads of unknown length (stdin) or larger than 5 GiB are needed, or placement must change mid-upload when a node dies (stage 6 onward).

## Coordinator-computed chunk boundaries; CompleteUpload validates against the issued plan (stage 2)
**Chose:** the coordinator decides each chunk's index and size: 4 MiB, except a shorter last chunk. `CompleteUpload` rejects with `INVALID_ARGUMENT` unless:
- `key` and `size_bytes` equal the stored values (proto3 can't tell an absent int64 from 0, so both must always be sent);
- the request's chunk IDs are exactly the set allocated to this object: none missing, none extra, none from another object, no duplicates;
- each chunk's index and size equal its allocated index and size (allocation produces 0…n−1, so this enforces contiguity);
- chunk sizes sum to the object size;
- each chunk's acknowledged nodes are non-empty, distinct, and a subset of that chunk's placement.

An unknown `object_id` returns `NOT_FOUND`. A rejected complete writes nothing.
**Still trusted from the client until stage 4:** that a placed node actually acknowledged the write. The coordinator never hears from the node. The bytes are unverified until stage 3.
**Over:** client-chosen chunk boundaries; validating only counts and totals.
**Because:** every chunk ID the coordinator records is one it issued (invariant 4), and checking against a recorded plan is the only way to catch a client that mixes up uploads.
**Breaks if:** a buggy client reports an ack that never happened. Metadata would then overstate (invariant 3) until stage 4 moves ack reporting off the client.

## Placement recorded at initiate in chunk_placements (stage 2)
**Chose:** a `chunk_placements (chunk_id, node_id)` table written at initiate. V2 backfills it from stage 1's `chunk_replicas`.
**Over:** an array column on `chunks`; not recording placement at all.
**Because:** it's what lets `CompleteUpload` refuse acks from non-placed nodes, and it gives stage 4 (three targets per chunk) and stage 7 (re-replication) a place to record targets. Foreign keys tie it to `nodes`.
**Breaks if:** placement becomes dynamic within one upload. Rows would then need updating mid-upload, which this design doesn't do.

## CompleteUpload is idempotent for an already COMMITTED object (stage 2)
**Chose:** completing an object that is already `COMMITTED` returns OK and changes nothing. That includes not moving the key pointer back if the key has since been overwritten.
**Over:** `FAILED_PRECONDITION`.
**Because:** a client whose complete succeeded but whose response was lost can retry safely. The object row lock serializes a retry against the original.
**Breaks if:** a client relies on a retried complete to re-point the key. It won't.

## Zero-byte objects have zero chunks (stage 2)
**Chose:** a 0-byte object has n = ⌈0 / 4 MiB⌉ = 0 chunks. Its commit is metadata only, with no node contact. `GetObject` LEFT JOINs to chunks so it still resolves, with an empty chunk list, rather than looking like `NOT_FOUND`. Stage 1's 0-byte objects keep their one empty chunk; `get` handles either.
**Over:** always one, possibly empty, chunk.
**Because:** one formula for every size; no empty files or RPCs. Invariant 2 holds trivially.
**Breaks if:** code assumes every object has at least one chunk. An inner join from objects to chunks is exactly that bug; mutation 4 checks for it.

## Proto: singular InitiateUploadResponse.chunk replaced by repeated chunks (stage 2)
**Chose:** delete `ChunkPlacement chunk = 2` with `reserved 2; reserved "chunk";`, add `repeated ChunkPlacement chunks = 3`, and add `index = 3` and `size_bytes = 4` to `ChunkPlacement`. Passes `buf breaking` (WIRE_JSON) against stage 1. Without the `reserved` lines, buf rejects the change on both number and name (checked).
**Over:** changing field 2 to `repeated` (JSON cardinality change, flagged by WIRE_JSON; an old singular parser *merges* repeated messages into one); keeping field 2 populated with chunk 0 for old clients.
**Because:** an old stage 1 client then sees no `chunk` and fails cleanly ("coordinator returned no nodes"), instead of uploading part of an object.
**Breaks if:** stage 1 clients had been deployed and still needed to work.

## Chunks upload sequentially in stage 2 (stage 2)
**Chose:** `osctl put` uploads chunks one after another. Each one is read through an `io.SectionReader` over its byte range, one fresh 1 MiB frame at a time, so memory stays at a few MiB whatever the file size. `get` writes chunks in index order to a temp file, then fsyncs and renames it.
**Over:** a bounded parallel upload (`errgroup`).
**Because:** there's one node, loopback bandwidth isn't the bottleneck, and stage 4's choice of write mechanism (client fan-out vs. primary forwarding) may reshape this path. Parallelism now is unmeasured.
**Breaks if:** throughput matters across real networks with several nodes.

## Superseded objects are left fully intact until stage 8 (stage 2)
**Chose:** after an overwrite, the old object stays `COMMITTED` with all its rows and chunk files. It's simply no longer referenced by `keys`. A reader that fetched its chunk list before the swap finishes normally.
**Over:** deleting at overwrite time; recording a `superseded_at` timestamp now.
**Because:** deleting at overwrite time would break in-flight readers, and GC is stage 8. Capturing the old object ID race-free during an upsert needs a retry loop, which is GC bookkeeping that belongs to stage 8.
**Breaks if:** disk fills. Every overwrite leaks the old object's bytes until stage 8, which will need a grace period before deleting a superseded object's files. Superseded objects are found as "COMMITTED and not referenced by `keys`."

## Atomicity is proven by a fault-injection trigger test; mutations as patches (stage 2)
**Supersedes:** "CompleteUpload atomicity is proven by the last-insert-failure test" (stage 1). With validation now running before any write, an unknown acknowledged node is rejected before anything is written, so that test can no longer prove atomicity.
**Chose:** `failedOverwriteLeavesOldObjectIntact` installs a test-only Postgres trigger that raises on the `keys` upsert, the last statement of `CompleteUpload`, after the replica inserts and status update have run. Production code has no hooks. The mutations are patches in `scripts/mutations/`, run by `scripts/coordinator-mutation-check.sh`:

| Mutation | Must be caught by |
|---|---|
| 1: `GetObject` ignores the pointer and status | `overwriteIsInvisibleUntilCommit` (and every validation case, which asserts a rejected upload stays invisible) |
| 2: no `@Transactional` on `completeUpload` | `failedOverwriteLeavesOldObjectIntact`, alone |
| 3: read-then-write key swap | `concurrentOverwritesAllSucceedAndLeaveOnePointer`, alone |
| 4: inner join to chunks | `zeroByteObjectResolvesWithEmptyChunkList`, alone |

Each validation case asserts its error message fragment as well as the status code, so it proves the specific check it targets.
**Over:** hand-editing code for each mutation check; rejecting at a later insert through bad input (impossible now).
**Because:** mutation checks in a script are repeatable by anyone, and the trigger makes "failure after earlier writes" reachable.
**Breaks if:** the order of statements in `CompleteUpload` changes so the key swap is no longer last, or code edits stop the patches from applying. The script then fails at `git apply --check`, loudly, rather than silently passing.

## Coordinator readiness: in-container /dev/tcp healthcheck; `up --wait` is the only wait (stage 2)
**Chose:** a Compose `healthcheck` on the coordinator that runs `bash -c 'exec 3<>/dev/tcp/127.0.0.1/9090'` inside the container, with `start_period: 60s` and `start_interval: 1s`. `docker compose up --wait` is the only readiness wait in scripts and verification commands. The gRPC server binds 9090 only after Flyway migrations and node registration complete, so an accepted connection means the coordinator is ready.

Verified on a clean `down -v`:
- The first probe failed (05:15:16.403). The server started at 05:15:16.437. The next probe passed (05:15:17.417). `--wait` returned at 05:15:17.935, and Flyway was already at V2 at that point.
- Negative control: with the probe pointed at a closed port, `up --wait --wait-timeout 15` exited 1 ("application not healthy after 15s").

**Over:**
- **No healthcheck.** Compose treats a running container as ready, so `--wait` returned before Flyway had run. A query in stage 2 verification raced V2 this way.
- **`nc -z 127.0.0.1 9090` from the host.** On Docker Desktop the published port is served by Docker's port proxy, which accepts the TCP connection as soon as the container exists, before the coordinator listens. A host-side probe therefore reports ready too early. It also adds a second, ad hoc wait mechanism outside Compose.
- **Grepping the logs for `Started CoordinatorApplication`.** This was used as a stopgap during stage 2 verification. It couples readiness to a log message's wording, and it only works for whoever runs the loop.
- **A gRPC health probe binary (`grpc_health_probe`).** It's a real application-level check, but it means an extra binary in the image, with its own arm64 build to track. A TCP accept already implies startup finished.

**Because:** the probe runs where the listener is, needs only bash (present in `eclipse-temurin:25-jre`, Ubuntu-based), and makes the stack itself define readiness.
**Breaks if:** the coordinator starts accepting connections before it can serve, for example if the gRPC server is ever started before migrations or registration. Or if the coordinator's runtime image moves to one without bash (for example distroless). Or if readiness needs more than "listening," such as a lost database connection. A TCP probe doesn't see that; the gRPC health service would.

## The chunk checksum travels in a trailer frame, hashed while streaming (stage 3)
**Chose:** `WriteChunkRequest.frame` gains `WriteChunkTrailer trailer = 3` (`bytes sha256 = 1`, exactly 32 bytes). A write is exactly one header, then data frames, then exactly one trailer, then end of stream. Data after the trailer, a repeated or missing trailer, or a digest that isn't 32 bytes → `INVALID_ARGUMENT`. The client hashes through an `io.TeeReader` as it sends, so the file is read once. Other new fields: `WriteChunkResponse.sha256 = 3` (the hash the node verified), `CompletedChunk.sha256 = 5`, `ChunkInfo.sha256 = 5`. All of these are additions, which pass `buf breaking` (WIRE_JSON). A deliberate renumber was checked to fail.
**Over:** the hash in the header frame.
**Because:** a header hash means reading each chunk twice or buffering 4 MiB on the client. The node gains nothing from knowing the hash early: it must receive every byte before it can compare, and it writes to a temp file meanwhile either way.
**Breaks if:** the node ever needs to reject a write before receiving it, based on its hash (for example deduplication, which is out of scope).

## Node write path: temp → fsync → link (no-replace) → fsync dir → unlink; temps cleaned before listening (stage 3)
**Supersedes:** "Node rejects malformed writes and never overwrites a chunk" (stage 1). The never-overwrite guarantee moves from `O_EXCL` on the final name to `link()` failing with `EEXIST`, and the crash gap that entry flagged is closed.
**Chose:** a write streams into `/data/tmp/chunk-<id>-*`, hashing as it goes. On the trailer, in `chunkstore.Writer.Commit`:
1. compare the hash; on a mismatch remove the temp file (nothing ever existed under the final name);
2. write the header, `fsync` the temp file, close it;
3. `link(temp, /data/chunks/<id>)`; `EEXIST` → remove the temp file, `ALREADY_EXISTS`, and the existing chunk is untouched;
4. `fsync /data/chunks`;
5. `unlink` the temp name;
6. acknowledge.

An `Exists` check at the header frame fails fast but is not the guarantee. At startup, before the listener opens, the node deletes everything in `/data/tmp` and logs each removal with its `chunk_id`. `/data/tmp` and `/data/chunks` are on the same volume, so `link()` works.

**Crash at any point:** before the link, only a temp file exists (removed at startup, never acknowledged). After the link, the final file is complete, because it was fsynced before it got the name. A leftover temp name is just a second link, removed at startup. No point leaves a partial file under a final name.
**Over:** plain `rename()`, which silently replaces an existing file and so breaks invariant 4; `renameat2(RENAME_NOREPLACE)`, which is Linux-only (macOS needs `renameatx_np(RENAME_EXCL)`), so `go test` on the Mac would exercise different code from the container; temp files as dotfiles inside `/data/chunks`.
**Because:** `link()` + `unlink()` is POSIX and refuses to replace on both platforms. Its cost, a moment with two names for one inode, is harmless.
**Breaks if:** two node processes share one data directory (startup cleanup would delete the other's in-flight temps), or the volume's filesystem lacks hard links.

## Self-describing chunk files: 56-byte header with magic, chunk ID and SHA-256 (stage 3)
**Chose:** each chunk file is `"OSCHUNK1"` (8 bytes, also the format version) + the chunk ID as 16 UUID bytes + the SHA-256 of the data (32 bytes) + the data.
**Over:** a separate hash file per chunk (two files, not created atomically: a crash between them leaves a chunk without a hash); extended attributes (support varies across Docker Desktop volumes and overlayfs); having the reader supply the expected hash from `GetObject` (the node couldn't verify anything on its own, and every caller, including stage 7 peer copies, would have to carry the hash).
**Because:** the node can verify any chunk by itself, before serving a client or a peer (invariant 5). The chunk ID in the header catches a *misplaced* file: an intact chunk sitting under another chunk's name. Its own hash would pass, so only the ID check catches it, and in stage 7 node-to-node copies no client is checking.
**Breaks if:** the format must change. The magic is the version, so a reader would have to accept both versions during a migration.

## Reads verify the whole chunk in memory before the first frame; DATA_LOSS on mismatch (stage 3)
**Chose:** `ReadChunk` reads the whole file into one buffer (at most 56 B + 4 MiB; anything else is corrupt), checks the magic, that the header's chunk ID equals the requested ID, and the SHA-256. Only then does it send ≤ 1 MiB slices of that same buffer. Any failure → `DATA_LOSS`, an error log line with `chunk_id` and `node_id`, and zero frames sent.
**Over:** two passes over the file (hash, then send, each with a 1 MiB buffer), where the bytes sent wouldn't be the bytes verified, because a media error or page-cache eviction between passes could serve unverified data; verifying per frame (needs per-frame hashes, and still sends bad frames before the end is checked).
**Because:** invariant 5 is about the bytes that actually go out, so verify-then-send must use one buffer.
**Breaks if:** chunks grow well beyond 4 MiB, or memory per read matters more than strict verification. See the next entry for the bound.

## Concurrent reads bounded by a semaphore; RESOURCE_EXHAUSTED when full (stage 3)
**Chose:** a buffered channel of capacity `NODE_MAX_CONCURRENT_READS` (default 32) in the `Server` struct. A read takes a slot without waiting, or fails at once with `RESOURCE_EXHAUSTED`, and gives the slot back when it finishes. Read memory is capped at about N × 4 MiB (128 MiB by default).
**Over:** unbounded reads (memory grows with demand until the node is OOM-killed); a waiting semaphore (requests pile up and hold their connections open, hiding overload from clients).
**Because:** failing fast tells the caller to back off or try elsewhere (stage 5 failover). The semaphore lives in the struct, so there's no package-level mutable state.
**Breaks if:** legitimate load regularly exceeds N, so clients see spurious errors. Writes are not bounded: each one holds only a 1 MiB frame, but write concurrency is still unlimited.

## A write whose hash doesn't match returns INVALID_ARGUMENT (stage 3)
**Chose:** `INVALID_ARGUMENT` when the received bytes don't match the trailer's SHA-256. The temp file is removed and nothing exists under the final name.
**Over:** `DATA_LOSS`.
**Because:** the request contradicts itself, and the client still has the data and can retry. gRPC defines `DATA_LOSS` as *unrecoverable* loss, which describes corrupt stored data on read. S3 (`BadDigest`, HTTP 400) and GCS make the same choice.
**Breaks if:** clients treat `INVALID_ARGUMENT` as permanent and don't retry what was really corruption in transit. Stage 4's write path decides retry behaviour.

## Checksums recorded at CompleteUpload from the client's report (stage 3)
**Amends:** the "still trusted from the client" list in "Coordinator-computed chunk boundaries; CompleteUpload validates against the issued plan" (stage 2).
**Chose:** migration V3 adds `chunks.sha256 bytea CHECK (octet_length(sha256) = 32)`. It's `NULL` while PENDING and set once at `CompleteUpload` (`... WHERE sha256 IS NULL`), in the same transaction as the replica rows, the status change and the key swap. `CompleteUpload` rejects a chunk whose `sha256` isn't exactly 32 bytes. `GetObject` returns each chunk's hash.
**Still trusted from the client until stage 4:** that a placed node acknowledged the chunk, and that the hash reported is the one the node verified. The client checks the node's `WriteChunkResponse.sha256` against its own hash, but the coordinator hears only the client. The coordinator can't verify the hash itself: it never sees the bytes.
**Over:** recording the hash at initiate (it isn't known yet); nodes reporting hashes to the coordinator directly (that's stage 4's ack-reporting question).
**Because:** this is the minimum that gives every committed chunk a recorded hash for the end-to-end check.
**Breaks if:** a buggy client reports a wrong hash. Every later `get` of that chunk then fails its end-to-end check: loud, but the object is unreadable.

## V3 requires a clean volume, enforced by a guard in the migration (stage 3)
**Chose:** V3 begins with a block that raises `stage 3 requires a clean volume: N existing chunks have no checksum. Run: docker compose down -v` if any `chunks` rows exist, so the coordinator refuses to start. Checked against a stage 2 schema with one chunk (it raised), and with none (it applied).
**Over:** serving pre-stage-3 chunks unverified (breaks invariant 5); hashing existing files now ("trust on first hash", which would bless any corruption already on disk); keeping `NULL`-hash chunks that can never be read.
**Because:** a hard upgrade boundary is explicit and loud. The alternatives are silently unsafe or silently broken.
**Breaks if:** a deployment has data worth keeping across this upgrade. That would need a migration tool that rewrites chunk files using hashes from a trusted source, which doesn't exist here.

## Absent checksum fields fail closed (stage 3)
**Chose:** a missing checksum is rejected everywhere, never read as "skip the check":
- a write with no trailer → `INVALID_ARGUMENT`;
- a stage 2 node sees the trailer as an empty frame and rejects the write;
- a `CompletedChunk` with no `sha256` → `INVALID_ARGUMENT`;
- `osctl` refuses a `GetObject` chunk list where any chunk lacks a 32-byte hash, and refuses a `WriteChunkResponse` whose hash differs from its own or is missing.

**Over:** treating an absent hash as unverified-but-allowed.
**Because:** "new fields must be safe when absent" means, for integrity fields, that absence must never weaken the check.
**Breaks if:** a mixed-version deployment must keep working across the upgrade. Every pairing of old and new fails here, deliberately.

## get verifies every chunk end to end against the coordinator's hash (stage 3)
**Chose:** `osctl get` writes each chunk through an `io.MultiWriter(tempFile, sha256)` and compares the result with `ChunkInfo.sha256` before moving on. A mismatch or a node `DATA_LOSS` fails the whole `get` loudly, naming the chunk and node, and no output file is created. Failover to another replica is stage 5.
**Over:** trusting the node's own check.
**Because:** the node's check catches corruption on disk. The client check catches what the node can't see: the wrong chunk served, a node bug, corruption in transit. It costs a streaming hash and no buffering.
**Breaks if:** never in correctness terms. Until stage 5, one bad replica makes the object unreadable.

## Write durability order is checked by a Go test seam and by a syscall trace (stage 3)
**Chose:** `chunkstore.Store` routes its four durability calls (`syncFile`, `link`, `syncDir`, `remove`) through function fields. `TestCommitDurabilityOrder` wraps them and asserts the exact sequence: sync temp → link tmp→chunks/<id> → sync dir chunks → unlink tmp. `scripts/stage3-fsync-trace.sh` attaches `strace` (side container, `--pid container:<node1>`, `SYS_PTRACE`) to the real binary and checks the same order from the syscalls (`fsync`, `linkat`, `fsync`, `unlinkat`) for each chunk of an upload. Mutation N3 (both fsyncs removed) is caught by the test, and by the trace against a node built with N3 applied (FAIL on both chunks, exit 1).
**Over:** the trace alone; no check at all (the page cache makes a missing fsync invisible to any ordinary test).
**Because:** the seam catches a missing or reordered call in `go test` in milliseconds. The trace proves the shipped binary makes the real syscalls. Neither can prove fsync is *sufficient* on a given disk; that needs power-loss testing (e.g. dm-flakey, CrashMonkey), which is out of scope.
**Breaks if:** someone adds a durability call that bypasses `ops`. The seam test would not see it, though the trace would.

## Node mutation script; coordinator script extended with M5 (stage 3)
**Extends:** "Atomicity is proven by a fault-injection trigger test; mutations as patches" (stage 2). Patches now live in `scripts/mutations/coordinator/` and `scripts/mutations/node/`.
**Chose:** `scripts/node-mutation-check.sh` applies each node patch, runs `go test -json ./node/...`, and reports failed tests. A mutation that doesn't compile is reported as `BUILD FAILED`, never as detected.

| Mutation | Must be caught by |
|---|---|
| N1 skip read hash check | `TestReadChunkCorruptSendsNothing` |
| N2 plain rename (and the unlink tolerates the vanished temp) | `TestWriteChunkNeverReplacesAtFinalStep`, alone |
| N3 no fsync (file and directory) | `TestCommitDurabilityOrder`, alone |
| N4 skip write hash check | `TestWriteChunkFraming/checksum_mismatch` |
| N5 no temp cleanup | `TestCleanTemps` |
| N6 link before comparing | `TestWriteChunkFraming/checksum_mismatch` |
| N7 skip chunk-ID check on read | `TestReadChunkCorruptSendsNothing/valid_chunk_file_under_another_chunk's_name`, alone |
| N8 no read limit | `TestReadChunkConcurrencyLimit`, alone |
| M5 (coordinator) hash not recorded | `checksumsAreRecordedAndReturnedByGetObject`, alone |

N2 is written as a *faithful* rename refactor. A bare `os.Rename` swap made every write fail at the following unlink, which "detected" it through all 25 tests without proving anything about replacement. `TestWriteChunkNeverReplacesAtFinalStep` waits for the server's temp file to appear, which shows the up-front `Exists` check has passed, and only then plants the final file, so the test exercises `link()` itself.
**Because:** each guarantee is shown to be load-bearing by a test that fails without it.
**Breaks if:** code changes stop a patch from applying. The script then fails at `git apply --check`, never silently.

## Stage 3 acceptance scripts reach the distroless node's volume through side containers (stage 3)
**Chose:** the node image has no shell, so the scripts run `alpine:3` with `--volumes-from <node1>` to read and modify `/data` (busybox `dd`, `od`, `sha256sum`), which also works while node1 is stopped.
- `stage3-corrupt-check.sh` flips one data byte, expects `DataLoss`, no output file and a node log line, then **flips it back** (even on failure) and checks the object reads back identical.
- `stage3-crash-check.sh` uses a `docker:cli` watcher with the Docker socket that busy-waits inside node1's volume for a temp file, then SIGKILLs node1 within milliseconds. It then checks every final chunk file (magic, ID, hash) and that this run's temp file is removed and logged after restart. It **fails if the kill landed too late**: a successful put, no temp file, or a `COMMITTED` object at step h each fail the run (checked by forcing a late kill).
**Over:** a debug shell in the node image; polling `/data/tmp` from the host with one `docker run` per poll (~300 ms per poll, longer than a whole 100 MiB upload).
**Because:** the production image stays minimal, and each script leaves the volume valid for the next. An earlier version of the corruption check didn't restore its byte, and the crash check correctly flagged that chunk as BAD.
**Breaks if:** the Docker socket isn't at `/var/run/docker.sock` inside the Docker VM, or the upload outruns the watcher on a much faster machine. The PENDING check then fails the run, never passes it.
