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

## Client fan-out writes; per-replica writer in a shared package (stage 4)
**Amends:** "Chunks upload sequentially in stage 2" (stage 2). Chunks are still written one after another; within a chunk, the three replicas are now written in parallel.
**Chose:** the client streams each chunk to its three placed nodes at the same time, each over its own independent stream. Each node verifies its own trailer SHA-256 (stage 3). The per-replica writer (header → frames → trailer → ack check) is `internal/chunkwrite` at the module root, so the node can reuse it for node-to-node copies in stage 7. The fan-out and quorum logic is `client/internal/upload`.
**Over:** primary-forwards (client → primary → secondaries, as a star or a chain).
**Because:**
- **Failures:** one failing replica affects only its own stream. With forwarding, a failed secondary must be reported back through the primary, and a failed primary fails all three even when two nodes are healthy.
- **Verification:** every replica checks the same trailer on its own, and no node ever passes on bytes it handled.
- **Bandwidth:** the cost is 3× client upload bandwidth, but nodes send nothing to each other on writes. GFS and HDFS chain writes to save client bandwidth, at the price of more complicated failure handling.

**Breaks if:** clients sit behind slow uplinks, where 3× upload matters more than simple failure handling.

## The coordinator verifies replicas by asking the placed nodes at CompleteUpload; the client's ack list is ignored (stage 4)
**Supersedes:** the "still trusted from the client" part of "Coordinator-computed chunk boundaries; CompleteUpload validates against the issued plan" (stage 2), and "Checksums recorded at CompleteUpload from the client's report" (stage 3). Acks are no longer the client's word.
**Chose:** `CompleteUpload` asks every placed node, in a `StatChunks` call, which of the object's chunks it durably holds. A replica counts only if its node reports a valid header with the planned size and the same SHA-256 as the client's `CompletedChunk.sha256`. The recorded hash is that agreed value. A node reporting a different hash or size is not counted, and is logged at error. If the client's hash matches no node, the commit is refused. `CompletedChunk.acked_node_ids` is `[deprecated = true]` and ignored. Asking every placed node, rather than the ones the client lists, also means a client bug can't lose a real replica.

`CompleteUpload` has three phases:
1. validate against the plan (reads only);
2. call `StatChunks` outside any transaction, so no row lock is held across the network;
3. in one transaction: lock, re-check PENDING, record exactly the verified replicas and the hash, mark COMMITTED, swap the key.

**Over:**
- **(a) Nodes report each write to the coordinator directly.** That puts the coordinator on every chunk's write path, and the node must report before acknowledging, or the complete can arrive first and be refused. Lost reports would also need handling.
- **(b) Signed ack tokens (an HMAC with per-node secrets, relayed by the client).** No extra RPCs, but secrets have to be distributed and rotated for a threat model (buggy clients, not malicious ones) that doesn't need forgery-proof acks. It's also close to authentication, which is out of scope.

**Because:** the coordinator gathers the facts itself, at the moment it commits. There's no race and no lost report, and it uses the coordinator→node path that stages 7 and 8 need anyway. A node that's dead or hung at commit time isn't counted. That's an under-count, which invariant 3 allows.
**Breaks if:** clients are malicious (anyone can call a node's RPCs; there's no authentication), or commit latency matters more than the one node round trip.

## StatChunks is one batched call per node, reading headers only (stage 4)
**Chose:** `StatChunks(chunk_ids)` takes every chunk of the object placed on that node and returns an entry per chunk: `FOUND` (size and hash from the header), `MISSING`, or `BAD_HEADER` (bad magic, a header naming another chunk, or an impossible size). Batches are capped at 4,096 IDs; a 5 GiB object has 1,280 chunks. The node checks the magic and the chunk ID but does not re-hash the data. Since stage 3 a file exists under a final name only after its hash was verified and it was fsynced, so a valid header *is* a durable verified write. Later corruption is caught at read time.
**Over:**
- **Per-chunk `StatChunk` calls, capped at 16 in flight.** A hung node holding about 960 chunks of a 5 GiB object would cost about 60 rounds of the 2 s deadline, around 120 s. Batched, it costs one deadline whatever the object's size. `hungNodeCostsOneDeadlineWhateverTheObjectSize` checks this with 40 chunks: it returns within the 500 ms test deadline plus a margin.
- **Re-hashing at commit.** That's 300 MiB of reads to commit a 100 MiB object, to recheck something the write path already proved.

**Breaks if:** one object's chunks on a single node exceed the batch cap. That needs objects far beyond the 5 GiB limit.

## Commit quorum: at least 2 verified replicas per chunk, else FAILED_PRECONDITION (stage 4)
**Chose:** `MIN_REPLICAS = 2`, enforced by the coordinator. Any chunk with fewer verified replicas refuses the whole commit with `FAILED_PRECONDITION`, naming the chunk and each node's reason ("missing", "bad header", "sha256 differs from the client's", "no answer"). Nothing is written, and the object stays PENDING. The client applies the same rule per chunk, and gives up on the upload as soon as a chunk gets fewer than 2 acks.
**Over:** `UNAVAILABLE` (it suggests retrying the same call, which won't help unless the nodes recover and the client rewrites the bytes); committing with fewer replicas and leaving stage 7 to repair them (that breaks invariant 2).
**Because:** invariant 2 (commit implies durability on ≥ 2 replicas) is now enforced by the side that records it.
**Breaks if:** a deployment wants per-object durability levels. Out of scope.

## No retry and no re-placement within an upload; orphaned bytes are left for stages 7 and 8 (stage 4)
**Chose:** a replica that fails isn't retried, and its chunk isn't moved to the fourth node.
- If 0 or 1 replicas fail, the upload continues.
- If 2 or 3 fail, the client gives up on the upload straight away: no further chunks, no `CompleteUpload`. The object stays PENDING and the key keeps its previous version.

Bytes that never reach metadata stay on the nodes:
- **Chunks of abandoned uploads.** They belong to PENDING objects, and `chunk_placements` says which nodes hold them. Stage 8 GC removes them.
- **A replica that finished after the client gave up on it, or whose node was down at commit.** The file is valid, but there's no replica row for it: an under-count that invariant 3 allows. Stage 7 re-replicates the chunk, and stage 8 must treat "file present, no replica row" as an orphan.

**Over:** retrying the same replica (how many times, how far apart?); re-placing onto the fourth node (it changes placement in the middle of an upload, which "All chunks allocated up front" from stage 2 rules out).
**Because:** stage 6's liveness-aware placement makes most of this unnecessary, and stage 7 repairs what's left.
**Breaks if:** short failures are common enough that whole uploads failing hurts more than retry logic would cost.

## Per-replica write deadline; a failed node is skipped for the rest of that upload (stage 4)
**Chose:** each replica write has a deadline covering connect, frames, trailer and ack: `OSCTL_CHUNK_WRITE_TIMEOUT`, default 10 s. A chunk waits for all three replicas or their deadlines. A node that fails or misses the deadline isn't tried again for the rest of that upload: later chunks count it as failed from the start. This state belongs to one upload in one client process; it isn't cluster liveness. A stopped container refuses the connection in milliseconds. A paused one accepts the TCP connection (the kernel's listen backlog still answers) and then never replies, so only the deadline catches it.
**Over:**
- **Cancelling the third replica as soon as two acknowledge.** Healthy but slightly slower nodes would routinely lose their replica, so "all 4 up gives 3 replicas" would fail.
- **A short grace period after the second ack.** That still costs the grace period on every chunk.
- **Waiting out the full deadline on every chunk.** A hung node would cost about 250 s for a 100 MiB upload.

**Because:** a hung node costs one deadline per upload. Measured with `docker compose pause node3`: a 10 MiB put took 12.2 s, which is the 10 s client deadline plus the coordinator's 2 s `StatChunks` deadline. `TestFailedNodeSkippedForRestOfUpload` checks that 5 chunks cost about one deadline.
**Breaks if:** a node recovers mid-upload. It won't be used again until the next upload, so that upload's later chunks get 2 replicas, not 3. Stage 7 restores them.

## Coordinator node calls: one deadline per CompleteUpload, outside the transaction, through cached channels (stage 4)
**Chose:** `NodeClient` sends one `StatChunks` call per node, all in parallel on virtual threads. They share one deadline: `COORDINATOR_NODE_RPC_TIMEOUT`, default 2 s. A node that fails, misses the deadline, or answers as another node is treated as no answer. Channels are plain grpc-java `ManagedChannel`s cached by `internal_addr`. They're only connections, rebuilt as needed, so invariant 7 holds.
**Over:** Spring gRPC's named client channels, which are configured statically, while node addresses come from Postgres.
**Because:** commit latency is bounded by one deadline, and no database row lock is held while waiting on the network.
**Breaks if:** many objects commit at once against a hung node. Each commit spends a deadline on it until stage 6 stops placing chunks there.

## Node responses must identify the responding node (stage 4)
**Chose:** a general rule: **any node response the coordinator records facts from carries the responding node's ID, and the coordinator rejects a response whose ID doesn't match the node it meant to call.** It logs an error and treats the response as no answer. `StatChunksResponse.node_id = 2` follows this rule now; stage 7's copy responses will too. CLAUDE.md invariant 3 now ends: "Facts about a node are recorded only from a response that identifies as that node." This is tested by `responseFromTheWrongNodeRecordsNothingForIt`, a fake node answering under another node's ID, and by mutation M10 ("accept a response from the wrong node").
**Over:** trusting that an address reaches the node it's registered for.
**Because:** addresses go stale. Stage 4 verification showed that stopping three nodes and starting them again can swap their container IPs; node2 and node3 traded .4 and .6. Here each node listens on a different port, so a stale address fails to connect. With the same port on every node, which is common, a stale address would reach a working node, and its chunks would be recorded as the wrong node's replicas: metadata overstating (invariant 3).
**Breaks if:** a node is misconfigured with another node's `NODE_ID`. That isn't detected, because there's no authentication.

## Coordinator DNS cache of about 1 s, plus channel reset and one retry on UNAVAILABLE (stage 4)
**Chose:**
- The coordinator sets the JVM's DNS cache times (`networkaddress.cache.ttl` and `networkaddress.cache.negative.ttl`, and the system property grpc-java's resolver reads) from `COORDINATOR_DNS_CACHE_TTL_SECONDS`, default 1. The defaults are 30 s for successful lookups and 10 s for failed ones.
- On `UNAVAILABLE` from `StatChunks`, the cached channel is shut down and replaced, so the address is looked up again, and the call is retried **once** within the same deadline. `StatChunks` only reads, so retrying is safe.

This is tested by `unavailableOnceIsRetriedAndTheReplicaCounts` (a fake node fails once, then answers, and the replica counts) and by mutation M11 ("no retry on UNAVAILABLE"). The smoke test is step 6 of `stage4-replication-check.sh`. Before the fix, two puts right after a three-node restart were refused ("node2: no answer; node3: no answer"). After it, in 6 stop/start rounds, 4 changed IPs, all 6 puts succeeded, and 8 resets and retries were logged.
**Over:**
- **B: fixed IPs for the nodes in Compose.** It removes the trigger here but leaves the coordinator just as fragile to address changes in any other deployment, and hard-coded addresses are extra configuration to keep in step.
- **C: a new channel for every commit.** Not enough on its own: the JVM's DNS cache would still hand the new channel the old address for up to 30 s. It would also pay a connection setup per node on every commit.
- **D: doing nothing.** Commits are refused for up to about 30 s after nodes restart with new IPs: safe, but spurious failures.

**Because:** this fixes the cause (stale resolution) where it lives, costs one quick retry only when something has failed, and never stretches past the commit's deadline.
**Breaks if:** DNS itself is slow or down. A 1 s cache means a lookup about every second for each node in use.

## Placement: rotation of 3 of the 4 nodes from a random start per object; stopped nodes are still placed until stage 6 (stage 4)
**Amends:** "Placement recorded at initiate in chunk_placements" (stage 2). There are now three rows per chunk.
**Chose:** nodes are sorted by `node_id`, and each object gets a random start `s`. Chunk `i` goes to nodes `s+i`, `s+i+1` and `s+i+2` (mod n). Each chunk leaves out one node, and consecutive chunks leave out consecutive nodes, so each node holds about ¾ of an object's chunks (`placementRotatesThreeOfFourNodes`). There's no liveness until stage 6, so a stopped node is placed like any other. With one node down, about ¾ of chunks include it and get 2 replicas. With two down, any 3 consecutive chunks include one that's placed on both, so an object of 3 or more chunks always fails.
**Over:** a random 3 of 4 per chunk (balanced only on average); rendezvous hashing on `chunk_id` (stable when nodes join or leave, but rebalancing is out of scope, and harder to reason about in acceptance checks).
**Because:** it's even, simple to explain, and its outcome with dead nodes can be predicted from `chunk_placements`, which the acceptance script does.
**Breaks if:** nodes are added or removed. Placement shifts for new objects, existing ones aren't moved, and that's out of scope.

## Stage 4 acceptance criteria corrected (stage 4)
**Chose:** CLAUDE.md's stage 4 *Accept* block is replaced:
- all 4 up gives 3 replicas, equal to placement, each verified on its own volume;
- with one stopped, replicas are placed minus stopped;
- with one paused, the upload finishes within deadline plus normal time;
- with three stopped, there's a clean failure;
- with two stopped, the per-chunk rule applies.

The original "with two stopped, uploads fail cleanly" was written for three nodes. With four nodes and two stopped, a chunk placed on both live nodes and one dead one still gets 2 acks and succeeds. `stage4-replication-check.sh` checks each criterion and works out the expected two-stopped outcome from `chunk_placements`.
**Over:** keeping the original wording, which isn't true for 4 nodes and isn't deterministic.
**Because:** acceptance criteria must be true and deterministic.
**Breaks if:** placement changes from rotation, because then "a 3-chunk object must fail with 2 stopped" no longer follows. The script's per-chunk prediction stays valid.

## Node readiness through a `health` subcommand and the gRPC health service (stage 4)
**Amends:** "Coordinator readiness: in-container /dev/tcp healthcheck; `up --wait` is the only wait" (stage 2). Nodes now have healthchecks too.
**Chose:** the node serves `grpc.health.v1`, set to `SERVING` only after temp cleanup has run and the listener is open. `/node health` calls `Health/Check` on its own `NODE_LISTEN_ADDR` with a 1 s deadline and exits 0 or 1. Compose runs it: `test: ["CMD", "/node", "health"]`, `start_period: 30s`, `start_interval: 1s`. `docker compose up --wait` now waits for all six services. The stage 3 crash check's "probe put" readiness trick is gone.
**Over:** a TCP-only probe (it would need a TCP-dialing mode in the binary anyway, and gRPC health is the protocol-level answer); a shell in the image (against the minimal-image choice).
**Because:** the distroless image has no shell, so the binary probes itself, and the health service is part of grpc-go, so there's no new dependency.
**Breaks if:** the node needs a richer "ready" state than "listening" (for example while loading state at startup). Then it would stay `NOT_SERVING` for longer.

## Mutation scripts extended; the Go script renamed (stage 4)
**Extends:** "Node mutation script; coordinator script extended with M5" (stage 3).
**Chose:** `scripts/node-mutation-check.sh` is renamed with `git mv` to `scripts/go-mutation-check.sh`, with patches moved to `scripts/mutations/go/`. It runs `go test ./...`, which covers the node, the client and `internal/`. New mutations:

| Mutation | Must be caught by |
|---|---|
| C1 client quorum of 1 | `TestWriteChunkQuorum` (the two-failing cases) |
| C2 no per-replica write deadline | `TestWriteChunkQuorum` (the hanging cases, failed by the test's own timer) |
| C3 a failed node isn't skipped | `TestFailedNodeSkippedForRestOfUpload` |
| N9 `StatChunks` skips the chunk-ID check | `TestStatChunks` |
| M6 coordinator quorum of 1 | `commitRefusedWhenAnyChunkHasFewerThanTwoVerifiedReplicas` |
| M7 record unverified replicas (every placed node) | `onlyVerifiedReplicasCount` "one missing" |
| M8 no node RPC deadline | `hungNodeCostsOneDeadlineWhateverTheObjectSize` (`@Timeout`) |
| M9 skip the hash comparison | `onlyVerifiedReplicasCount` "one with another hash" |
| M10 accept a response from the wrong node | `responseFromTheWrongNodeRecordsNothingForIt` |
| M11 no retry on UNAVAILABLE | `unavailableOnceIsRetriedAndTheReplicaCounts` |

M2 (no transaction) is regenerated: the commit phase now runs through a `TransactionTemplate`, and the mutation calls it directly. M5 is regenerated against the restructured service.
**Because:** every new guarantee is shown to be load-bearing.
**Breaks if:** code edits stop patches from applying. The scripts then fail at `git apply --check`, never silently.

## Stage 3 acceptance scripts adapted to replication; scripts never hide osctl errors (stage 4)
**Amends:** "Stage 3 acceptance scripts reach the distroless node's volume through side containers" (stage 3).
**Chose:**
- **corrupt-check:** flips the byte on the replica `get` reads first, the lowest `node_id`, since there's no failover until stage 5.
- **crash-check:** killing node1 mid-upload no longer fails the upload. It now must commit, with node1 recorded for no chunk from the interrupted one onwards, every chunk on ≥ 2 replicas, and `get` identical. The kill-too-late guard is the temp file in step c: an upload that finished before the kill leaves none. Readiness comes from node1's healthcheck.
- **fsync-trace:** expects as many links on node1 as `chunk_placements` lists for it.
- **Every script:** osctl calls go through `scripts/lib/osctl.sh`'s `osctl_run`, which prints the command and osctl's error and warning lines on failure. No script sends osctl's errors to `/dev/null` any more.
- **overwrite-check:** reports disk usage on all four node volumes.

**Because:** an adapted corruption check died silently. Its first put had been refused because of the stale-DNS problem above, and the error went to `/dev/null`.
**Breaks if:** new scripts call osctl directly.

## Reads shuffle each chunk's replicas; OSCTL_READ_ORDER=listed for deterministic reads (stage 5)
**Amends:** "Stage 3 acceptance scripts adapted to replication; scripts never hide osctl errors" (stage 4), whose corrupt-check relied on `get` reading the lowest `node_id` first.
**Chose:** `GetObject` returns a chunk's HEALTHY replicas in a stable order (by `node_id`). The client shuffles each chunk's list before reading (`download.Shuffled`). `OSCTL_READ_ORDER=listed` keeps the coordinator's order, so scripts know which replica is read first. Go tests inject the order function directly.
**Over:** always reading the lowest `node_id` first (every read of a chunk lands on one node, and node1 is first for about half of all chunks); one shuffle per `get` (spreads load only across gets); shuffling in the coordinator (puts read policy in the metadata service and makes `GetObject` non-deterministic for its own tests); forcing a replica in scripts by watching logs in a retry loop (non-deterministic, and the steps must know the first replica before corrupting it).
**Because:** spread load by default, determinism on request.
**Breaks if:** reads need to prefer a node for locality or cost. Then the order would need a real policy, not just a shuffle.

## Failover and report classification, with an explicit default (stage 5)
**Chose:** `download.classify` decides what each failed replica read means.

| Outcome | Fail over | Report |
|---|---|---|
| `UNAVAILABLE`, `DEADLINE_EXCEEDED`, `RESOURCE_EXHAUSTED` | yes, and the node is demoted for the rest of the get | no |
| a stream that breaks mid-chunk (surfaces as one of the codes above, or as an unlisted code) | yes | no |
| `NOT_FOUND` | yes | yes (`REASON_NOT_FOUND`) |
| `DATA_LOSS` | yes | yes (`REASON_DATA_LOSS`) |
| wrong size or SHA-256 at the client | yes | yes (`REASON_CHECKSUM_MISMATCH`) |
| `INVALID_ARGUMENT` | **no**: the get fails; every replica would refuse a malformed request | no |
| **any other code** (`INTERNAL`, `UNKNOWN`, `UNIMPLEMENTED`, and so on) | yes | **no** |

The last row is an **explicit `default:` branch** in the switch, not whatever falls through: an unlisted code isn't proof the replica is bad. Our own cancelled context (Ctrl-C) stops the get. A replica is reported only when it served wrong data, or claims not to have a chunk it's recorded as holding. A stopped, slow or busy node is a liveness matter (stage 6), not a bad replica. Tested by `TestFailoverClassification`, one case per row, including an `INTERNAL` replica (fails over, identical output, no report), and by `TestInvalidArgumentDoesNotFailOver`. Mutations: D2 (report on `UNAVAILABLE`), D5 (fail over on `INVALID_ARGUMENT`), D6 (an unlisted code is reported).
**Over:** reporting every failure (a stopped node would get its replicas marked bad); failing the get on an unknown code (one odd node could make an object unreadable when other replicas are fine).
**Because:** failover is cheap and safe, while a report ends in a permanent BAD mark if the node confirms it, so reports need evidence.
**Breaks if:** a node returns `INTERNAL` for a corrupt file. It would never be reported, and only stage 7's checks or another reader's `DATA_LOSS` would catch it.

## Per-replica read deadline; demotion for the rest of the get (stage 5)
**Chose:** each replica read has a deadline covering connect and the whole stream: `OSCTL_CHUNK_READ_TIMEOUT`, default 10 s. A replica that fails with `UNAVAILABLE`, `DEADLINE_EXCEEDED` or `RESOURCE_EXHAUSTED` is tried **last** for the rest of that get. It's demoted, not dropped, so it's still the last resort if every other replica fails.
**Over:** dropping the node for the rest of the get, as uploads do (on a read, the dropped node might be the only replica that would have worked); no demotion (a hung node first in the order for every chunk costs 10 s per chunk).
**Because:** a hung node costs one deadline per get. `TestHungReplicaDemotedForRestOfGet` checks 5 chunks in about one deadline (mutation D4). On a paused node, a 10 MiB get took 10.2 s.
**Breaks if:** many gets run against a hung node at once. Each one pays a deadline until stage 6 stops directing reads there.

## Chunks are buffered in memory and verified before anything is written (stage 5)
**Amends:** "get verifies every chunk end to end against the coordinator's hash" (stage 3). Bytes no longer go straight to the temp file.
**Chose:** each replica attempt reads into a per-get buffer, reset to length 0 before every attempt, with a fresh SHA-256 hasher. Frames beyond the chunk's size are a mismatch. Only after the size and hash match are the bytes written to the temp file. Memory is at most 4 MiB per get, because chunks are read one at a time and the buffer is reused.
**Over:** writing straight to the temp file and, on failover, truncating and seeking back to the chunk's offset and resetting the hasher (1 MiB of memory, but unverified bytes sit on disk, and a rewind bug corrupts output silently); one temp file per chunk attempt, appended on success (every chunk written twice).
**Because:** the temp file only ever holds verified chunks, so there's nothing to rewind. Tested by the "stream breaks mid-chunk" case: two frames, then `UNAVAILABLE`, then byte-identical output from the next replica. Mutation D1 (no reset on failover) is caught by it.
**Breaks if:** chunks are read in parallel. Memory then becomes the number of chunks in flight × 4 MiB.

## A client's bad-replica report is a hint; the node confirms it before anything is marked (stage 5)
**Extends:** "Node responses must identify the responding node" (stage 4), to `VerifyChunk`.
**Chose:** `ReportBadReplica(chunk_id, node_id, reason, detail)` doesn't mark anything by itself. If the replica isn't recorded → `NOT_A_REPLICA`. If it's already BAD → `ALREADY_BAD`. Otherwise the coordinator calls the new node RPC `VerifyChunk(chunk_id, expected_sha256)`, with the hash from `chunks.sha256`, within the node RPC deadline and with the stage 4 retry on `UNAVAILABLE`. The node re-reads the whole file: magic, chunk ID, data against the stored hash, and the stored hash against the expected one.

| The node's answer | Effect | Verdict |
|---|---|---|
| `CORRUPT` | marked BAD, reason "corrupt: <node's detail>" | `MARKED_BAD` |
| `MISSING` | marked BAD, reason "missing" | `MARKED_BAD` |
| `HEALTHY` | nothing; logged as contradicted (transit corruption or a client bug) | `CONTRADICTED` |
| no answer | nothing | `UNCONFIRMED` |
| answer from the wrong node | nothing; error log | `UNCONFIRMED` |

The mark is `UPDATE … WHERE state = 'HEALTHY'`, so of two concurrent reports exactly one marks; the rest get `ALREADY_BAD`. Tested in `BadReplicaReportIT`. Mutations: M12 (mark without confirmation) and M14 (accept a `VerifyChunk` answer from the wrong node).
**Over:** marking on the client's word (breaks invariant 3); confirming with `StatChunks` (it reads only the header, so it can't see data corruption); the coordinator reading the chunk itself (breaks invariant 6).
**Because:** "this replica is bad" is a fact about a node, so per invariant 3 it's recorded only from a response that identifies as that node.
**Breaks if:** a node is down when the report arrives. The report is dropped (`UNCONFIRMED`) and not retried. The next reader that hits the bad replica reports it again.

## Reports are sent after the get, are bounded, and never affect its outcome (stage 5)
**Chose:** `osctl get` collects reports while reading and sends them once reading has finished, whether the get succeeded or not. Each has a 5 s deadline. Each verdict is logged. A failed report is a warning line and nothing more: it never changes the get's result or exit status.
**Over:** fire-and-forget goroutines (the CLI may exit before they finish, and the reports are lost); reporting synchronously at the moment of failover (adds the confirmation round trip, including the node's re-hash, to the get's latency).
**Because:** reading stays as fast as failover allows, and reports still reliably reach the coordinator from a short-lived CLI.
**Breaks if:** a get is killed (`SIGKILL`) before its reports are sent. Those reports are lost, and the next reader reports again.

## Replica state on chunk_replicas; BAD is terminal (stage 5)
**Chose:** migration V4 adds `state` (`HEALTHY` or `BAD`, default `HEALTHY`), `bad_reason` and `marked_bad_at` (set with `clock_timestamp()`), with a CHECK that BAD rows have both. There's no transition from BAD back to HEALTHY.
- Stage 7 counts and copies only HEALTHY replicas.
- Stage 8 finds BAD rows, deletes the file on that node, then deletes the row.

**Over:** deleting the row (stage 8 would lose track of where the bad file is, "placed but no row" would mean both "never acknowledged" and "went bad", and the reason would be lost); a separate `bad_replicas` table (rows moving between tables must stay consistent, for no gain).
**Because:** one row tracks one copy for its whole life.
**Breaks if:** a node bug produces a false `CORRUPT`. There's no way to reinstate the replica short of manual SQL. Stage 7 would just make a new copy elsewhere.

## GetObject excludes BAD replicas; DATA_LOSS when a chunk has none healthy (stage 5)
**Amends:** "Key pointer table with last-commit-wins atomic swap" (stage 2): the read is still one statement, with the replica join now `ON … AND r.state = 'HEALTHY'`.
**Chose:** BAD replicas are never listed. If any chunk is left with no healthy replica, `GetObject` fails with `DATA_LOSS`: "chunk N (id) has no healthy replicas: node1: corrupt: …; …". No download starts.
**Over:** listing BAD replicas last (a confirmed-bad replica can't serve verified bytes, so trying it wastes an attempt and repeats the report); returning an empty replica list and letting the client fail (later, with less context).
**Because:** readers are never sent to a known-bad copy, and an unreadable object fails at once with the recorded reasons. Mutations: M13 (`GetObject` ignores the state) and M15 (no `DATA_LOSS`).
**Breaks if:** every replica of a chunk is wrongly marked BAD (see the previous entry). The object is then unreadable, even though a file might still be good.

## Bad files stay on disk until stages 7 and 8; no quarantine (stage 5)
**Chose:** a BAD replica's file is left where it is. A file that fails the node's own check keeps getting refused with `DATA_LOSS`. A file that passes the node's check but isn't the recorded chunk (a stale or misplaced file) would still be served by the node, but `GetObject` no longer lists that replica, and stage 7 copies only from healthy ones, so nothing is sent to it.
**Over:** the node moving the file to `/data/bad/` on the coordinator's word. Renaming doesn't change the contents, so invariant 4 holds either way, but it adds a coordinator-driven RPC that changes node state ahead of stage 8, which owns deletion and its safety rules.
**Because:** stage 5 is about reading correctly. Removing files belongs to stage 8.
**Breaks if:** disk space matters before stage 8. BAD files take up space until then.

## VerifyChunk: a full re-hash against the header and the coordinator's hash, counted against the read limit (stage 5)
**Chose:** `VerifyChunk` reuses `store.Read`, which checks the magic, the chunk ID and the data hash. It then compares the stored hash with `expected_sha256`. It answers `HEALTHY`, `CORRUPT` (with detail) or `MISSING`, plus `node_id` and `chunk_id`. It takes a read slot like `ReadChunk`, so a flood of reports can't add unbounded re-hash work; when the node is full it returns `RESOURCE_EXHAUSTED`, which the coordinator treats as no answer. Mutation N10 (ignore `expected_sha256`) is caught by `TestVerifyChunk/stored_hash_differs_from_expected`.
**Over:** checking the header only (that's `StatChunks`, and it can't see data corruption); verifying only against the node's own header (a valid file under the wrong chunk's name, or a stale file, would pass).
**Because:** confirmation must be as strong as the claim being confirmed.
**Breaks if:** reports arrive faster than nodes can re-hash. Then they come back `UNCONFIRMED`.

## Mutation scripts extended for stage 5 (stage 5)
**Extends:** "Mutation scripts extended; the Go script renamed" (stage 4).
**Chose:**
- **Go:** D1 no reset on failover; D2 report on `UNAVAILABLE`; D3 no read deadline; D4 no demotion; D5 fail over on `INVALID_ARGUMENT`; D6 an unlisted code is reported; N10 `VerifyChunk` ignores the expected hash. Each mutation names a target test, or a single subtest such as `TestFailoverClassification/internal_(unlisted_code)`.
- **Coordinator:** M12 mark BAD without confirmation; M13 `GetObject` ignores the state; M14 `VerifyChunk` accepts a response from the wrong node; M15 no `DATA_LOSS` without a healthy replica.

The coordinator script now totals every JUnit report, because there are two test classes. Patches M1, M4 and M11 are regenerated against the changed repository and `NodeClient`.
**Because:** every new guarantee is shown to be load-bearing.
**Breaks if:** code edits stop patches from applying. The scripts fail at `git apply --check`.

## stage3-corrupt-check corrupts every replica; shared volume helpers (stage 5)
**Amends:** "Stage 3 acceptance scripts adapted to replication; scripts never hide osctl errors" (stage 4).
**Chose:** with failover, one corrupt replica no longer fails a get (`stage5-failover-check.sh` covers that). To keep stage 3's criterion (a flipped byte makes get fail loudly, never return bad data), `stage3-corrupt-check.sh` now corrupts **every** replica of one chunk. It expects:
- the get fails, naming each replica's `DataLoss`, and leaves no output file;
- each node logs its verification failure;
- the untouched object is still identical;
- the get's reports mark all three replicas BAD.

It then flips the bytes back, so every file on the volumes is valid again, and checks that the object now fails at `GetObject` with `DATA_LOSS`, because BAD is terminal. The flip and delete helpers live in `scripts/lib/volume.sh`, shared with `stage5-failover-check.sh`.
**Because:** the criterion stays testable after the behaviour it was written against has changed.
**Breaks if:** an object needs to stay readable after the script runs. It doesn't, by design.

## Nodes push heartbeats; the node dials the coordinator with a fresh name lookup on every reconnect (stage 6)
**Chose:** every node calls `CoordinatorService.Heartbeat(node_id, instance_id)`: once at startup, then every `NODE_HEARTBEAT_INTERVAL` (2 s) ±10 %. The coordinator records the receipt time on its own clock. The node dials `COORDINATOR_ADDR` through grpc-go's `passthrough` resolver, so every reconnect asks the OS resolver afresh (Go keeps no DNS cache): a coordinator restarted on a new IP costs at most one failed beat. gRPC's own reconnect backoff is capped at one interval (`heartbeat.Dial`). With grpc-go's default (base 1 s, growing to 120 s), a node would stay silent long after an outage ended, and go falsely DEAD. Measured: after a 2 s outage, the default delayed the first beat by 589 ms against a 200 ms bound; mutation H4.
**Over:** pull (the coordinator probing each node). It needs a custom RPC that carries identity, since the standard gRPC health response has none; a scheduler in the coordinator; and it would add a second liveness signal on the coordinator→node path that stage 4 showed can go stale.
**Because:** it's the plan's design, a dead node falls silent by itself, and the coordinator stays passive.
**Breaks if:** a node can reach the coordinator but the coordinator can't reach the node (for example a stale `internal_addr`). The node is then ALIVE but unreachable. That's tolerated, because every coordinator→node call treats a failure as no answer (an under-count).

## Membership stays COORDINATOR_NODES; heartbeats from unknown nodes are rejected (stage 6)
**Amends:** "Node registry seeded from COORDINATOR_NODES at startup" (stage 1). Its expectation that stage 6 would replace seeding with heartbeat registration is deliberately not followed.
**Chose:** heartbeats update liveness only, never membership or addresses. A beat from a `node_id` not in `COORDINATOR_NODES` → `NOT_FOUND` ("node9 is not in COORDINATOR_NODES"), and nothing is recorded. The node logs it and keeps beating, so fixing the config lets it in without a restart. Mutation L3.
**Over:** nodes registering themselves.
**Because:** a misconfigured process would add itself as a placement target, nodes would need to know their own client-facing address, and adding nodes and rebalancing are out of scope.
**Breaks if:** membership must change at runtime. That means a config change and a coordinator restart.

## A per-process instance_id; a replaced instance that beats again means a conflict (stage 6)
**Chose:** each node process sends a random `instance_id`.
- A new instance is treated as a **restart** and accepted at once, even within seconds of the old one: `docker compose restart` must not look like a conflict.
- If an instance that was **replaced** beats again, two processes claim the node. Every beat for that node (from any instance) gets `FAILED_PRECONDITION` until only one instance has beaten for `DEAD_AFTER`. Rejected beats aren't signs of life, so the node goes SUSPECT, then DEAD, then recovers normally once one instance is left.

Mutation L7.
**Over:** treating any new instance as a conflict while the old one is recent (a fast restart would be refused for 15 s); accepting both instances (placement could reach the wrong process, so metadata could overstate).
**Because:** the coordinator can't tell which process is the real node, so neither gets placed on.
**Breaks if:** a restart reuses an instance ID. It can't: the ID is 128 random bits per process.

## The liveness state machine and its clock (stage 6)
**Chose:**
- UNKNOWN →(first beat)→ ALIVE;
- ALIVE →(6 s of silence)→ SUSPECT →(one beat)→ ALIVE;
- ALIVE or SUSPECT →(15 s of silence)→ DEAD;
- UNKNOWN →(no beat for 15 s since coordinator start)→ DEAD.

All of it is configurable: `COORDINATOR_SUSPECT_AFTER`, `COORDINATOR_DEAD_AFTER`, `COORDINATOR_RECOVERY_BEATS`, and a 1 s `COORDINATOR_LIVENESS_SWEEP` that applies timeouts and persists transitions. Startup requires 0 < SUSPECT < DEAD. **The clock is the coordinator's own monotonic `System.nanoTime()` at receipt, behind an injectable `Ticker`.** Heartbeats carry no timestamp. Measured with the Compose defaults: node2 was SUSPECT 5.3 s and DEAD 14.4 s after `docker compose stop`. Mutation L1.
**Over:** node-supplied timestamps (clock skew would decide liveness); phi-accrual failure detection (more than four nodes on a LAN need).
**Because:** the coordinator is the only judge, and only its own elapsed time can be trusted.
**Breaks if:** the coordinator itself stalls (for example a long GC pause). Beats arrive late, and nodes look silent.

## Hysteresis: SUSPECT recovers on one beat, DEAD on two consecutive beats (stage 6)
**Chose:** from SUSPECT, one beat → ALIVE. From DEAD, `COORDINATOR_RECOVERY_BEATS` (2) beats are needed, each within `SUSPECT_AFTER` of the previous; a longer gap starts the count again. With the defaults, a restarted node is ALIVE about one interval after its first beat. Mutation L4.
**Over:** a single beat to leave DEAD (a node that flickers on would be placed, fail, and be placed again).
**Because:** SUSPECT means "maybe slow", which one beat disproves. DEAD means "gone", which needs a little more evidence.
**Breaks if:** heartbeats are lossy enough that two in a row rarely arrive within 6 s. The node then never recovers, which would mean the threshold is wrong.

## UNKNOWN after a coordinator restart; never DEAD before a full window (stage 6)
**Chose:** in-memory liveness is empty after a restart. Every node starts **UNKNOWN**, and startup persists that. UNKNOWN becomes ALIVE on its first beat (no hysteresis: there's no DEAD history to distrust), and DEAD only after `DEAD_AFTER` with no beat since the coordinator started. Placement treats UNKNOWN as not ALIVE, so initiates can fail with `UNAVAILABLE` for up to about one interval after a restart. Measured: no node was DEAD at any 0.5 s sample over the 15 s after a coordinator restart, and all four were ALIVE 2.9 s after it was serving. Mutation L5.
**Over:** starting every node ALIVE (placement could pick a dead node for the first 15 s); trusting the persisted pre-restart state (stale by definition, and invariant 7 says it's rebuilt from heartbeats).
**Because:** a restart must never declare a node DEAD, and must never place on a node it hasn't heard from.
**Breaks if:** clients can't tolerate a few seconds of `UNAVAILABLE` after a coordinator restart.

## Liveness lives in memory and is persisted for observability and stage 7; what stage 7 can rely on (stage 6)
**Chose:** the in-memory `LivenessTracker` is authoritative. Migration V5 adds `nodes.state`, `state_changed_at`, `last_heartbeat_at` and `instance_id`. They're written on every transition (`last_heartbeat_at` at most every 10 s per node), with `clock_timestamp()`. They're wall-clock values for people and scripts only, and never drive transitions.

Stage 7 can rely on:
- the tracker's current state per node;
- "DEAD since", measured by this coordinator's monotonic clock;
- after a coordinator restart, "DEAD since" restarting from zero: a node can only be DEAD again after a full window, which delays repair, never hastens it;
- never acting on UNKNOWN;
- DEAD meaning "no heartbeat reached this coordinator for `DEAD_AFTER`", not "the data is gone".

**Over:** persisting every beat (2 writes per second for no reader); Postgres as the liveness authority (invariant 7: in-memory state rebuilt from heartbeats).
**Because:** decisions stay on monotonic time, and the facts stay inspectable.
**Breaks if:** something reads `nodes.state` as authoritative. It can lag the tracker by up to one sweep (about 1 s).

## Placement uses only ALIVE nodes; with 2 ALIVE, 2 replicas; with fewer, UNAVAILABLE (stage 6)
**Supersedes:** "Placement: rotation of 3 of the 4 nodes from a random start per object; stopped nodes are still placed until stage 6" (stage 4).
**Chose:** rotation as before, but over **ALIVE** nodes only, sorted by `node_id`.

| ALIVE nodes | Placement |
|---|---|
| 3 or more | 3 replicas per chunk |
| exactly 2 | 2 replicas on those 2 (quorum reachable; warn log; stage 7 tops up) |
| fewer than 2 | `UNAVAILABLE`: "N of M storage nodes alive, need 2" |

A 0-byte object places nothing, so it still commits with no node alive. Mutation L2.
**Over:** refusing uploads when only 2 nodes are ALIVE (trades availability for a durability gap stage 7 closes); placing on SUSPECT or UNKNOWN nodes (writes would aim at nodes likely to fail).
**Because:** writes go where they can succeed, and with 2 nodes the commit quorum is still met.
**Breaks if:** many objects are written while only 2 nodes are ALIVE. They're under-replicated until stage 7 catches up.

## CompleteUpload skips DEAD nodes when asking StatChunks (stage 6)
**Amends:** "The coordinator verifies replicas by asking the placed nodes at CompleteUpload; the client's ack list is ignored" (stage 4).
**Chose:** `StatChunks` goes to every placed node that isn't DEAD, which includes SUSPECT and UNKNOWN ones. A DEAD node is skipped, and its reason reads "DEAD, not asked". Mutation L6.
**Over:** asking only ALIVE nodes (a brief SUSPECT blip would refuse a commit that should succeed); asking DEAD nodes too (a hung DEAD node costs the full 2 s deadline on every commit).
**Because:** a briefly SUSPECT node usually still holds its chunks. A DEAD one is either stopped (the call would fail fast) or hung.
**Breaks if:** a node is DEAD by heartbeat but serving and holding the chunk. Its replica isn't recorded: an under-count, which invariant 3 allows.

## GetObject reports each replica's liveness; the client reads by liveness first and never drops a replica (stage 6)
**Amends:** "Reads shuffle each chunk's replicas; OSCTL_READ_ORDER=listed for deterministic reads" and "Per-replica read deadline; demotion for the rest of the get" (stage 5). The latter's "until stage 6 stops directing reads there" is done here.
**Chose:** `NodeLocation.liveness = 3` is set on every replica. The client groups a chunk's replicas:
1. ALIVE (or unreported);
2. SUSPECT and UNKNOWN;
3. DEAD;
4. nodes already demoted in this get.

Within each group it shuffles, or keeps the listed order. A DEAD replica is read only after every live one fails, never skipped. Measured: with node2 DEAD and first in listed order for a chunk, a get took 0.19 s against a 0.15 s baseline, with no failover. Mutation R1.
**Over:** the coordinator just listing non-ALIVE replicas last (the client's shuffle would scramble that); dropping non-ALIVE replicas (liveness can be wrong).
**Because:** read policy stays in one place, the client, and acts on facts the coordinator supplies.
**Breaks if:** liveness is systematically wrong (for example nodes that can't reach the coordinator but serve clients fine). Reads would then avoid healthy replicas until they're the only ones left.

## The node's heartbeat loop: fixed interval with jitter, a per-beat timeout, no backoff, clean shutdown (stage 6)
**Chose:** `node/internal/heartbeat.Run` runs in its own goroutine and never touches the serving path.
- It beats immediately at startup, then every interval ±10 %.
- Each beat has a timeout of one interval, so at most one is in flight.
- When the coordinator is down, it keeps the **same interval with no exponential backoff**, and gRPC's reconnect backoff is capped to match. Failures are logged at the first one, then at most every 30 s, plus once on recovery.
- On SIGTERM, `main` cancels the loop, waits for it (at most one beat's timeout), then calls `GracefulStop`.

Tests use a fake coordinator: immediate first beat, the interval, surviving a 2 s outage, a hung coordinator (one beat in flight), rejections, and prompt shutdown. Mutations H1, H2, H3, H4.
**Over:** exponential backoff (a node in a long backoff when the coordinator returns would sit UNKNOWN, then could go DEAD for no reason); a final "going away" beat that marks the node DEAD at once (a clean stop and a crash should take the same path through the state machine).
**Because:** a beat is cheap, and the coordinator needs beats promptly when it comes back.
**Breaks if:** many nodes beat a struggling coordinator. Then there's no backoff to shed load. Four nodes at 0.5 beats per second each is nothing.

## `up --wait` means the coordinator is serving; ready for uploads means wait_alive (stage 6)
**Amends:** "Coordinator readiness: in-container /dev/tcp healthcheck; `up --wait` is the only wait" (stage 2), and "Node readiness through a `health` subcommand and the gRPC health service" (stage 4).
**Chose:** healthchecks stay local. The coordinator is healthy when its gRPC server listens; a node is healthy when its own health service is SERVING. **Neither depends on liveness.** "Ready for uploads" is a separate condition: enough ALIVE nodes. `scripts/lib/liveness.sh` provides `wait_alive N [NODE…]` (at least N ALIVE, or every named node ALIVE) and `wait_state NODE STATE TIMEOUT`. Both poll the persisted `nodes.state`, time out (30 s by default), and print the last states seen.

Every script that starts services, a node or the coordinator and then puts calls it before the put:
- **the start of every acceptance script:** stage 2 round-trip and overwrite, stage 3 corrupt, crash and fsync-trace, stage 4, stage 5 and stage 6;
- **stage 4:** after restarting node2 (step 2), unpausing node3 (step 3), each step 4 cycle, and in step 6, where "put immediately" now means right after `wait_alive 4`;
- **stage 3 crash-check:** after restarting node1;
- **stage 5:** after restarting (step 3) and unpausing (step 4) the first replica's node;
- **stage 6:** throughout.

**Over:** making the coordinator's healthcheck depend on liveness (it would turn unhealthy whenever nodes are stopped on purpose, which several scripts do, and `up --wait` would then fail).
**Because:** "the process is serving" and "the cluster can take an upload" are different facts, and Compose can only wait on the first.
**Breaks if:** a script calls `wait_alive` before defining `$compose`. `liveness.sh` then reports that loudly; that bug showed up during stage 6 verification and is fixed.

## Scripts are deterministic again under liveness (stage 6)
**Amends:** "Stage 4 acceptance criteria corrected" (stage 4), the stage 4 acceptance-script entry, and "stage3-corrupt-check corrupts every replica; shared volume helpers" (stage 5). CLAUDE.md's stage 4 criteria remain true under liveness; only which branch each step exercises has changed.
**Chose:** each step either waits for a known state, or checks a precondition that shows which side of detection it landed on.
- **`stage4-replication-check.sh`:**
  - *Step 2:* waits for node2 DEAD. It's never placed, and every chunk gets 3 replicas from the live nodes.
  - *Step 3:* pauses and puts at once, and requires node3 to be in the placement (otherwise "inconclusive, rerun"), keeping the write-deadline path tested.
  - *Step 4:* classifies each attempt by placement width (3 before detection, 2 after) and runs up to 3 stop/restart cycles until both branches and the 3-chunk failure are seen before detection. After detection, a 3-chunk object must succeed with exactly node1 and node2 on every chunk.
  - *Step 5:* waits for three DEAD, then expects `UNAVAILABLE` at initiate.
  - *Step 6:* waits for ALIVE after `up --wait`.
- **`stage5-failover-check.sh` steps 3–4:** right before the get, the stopped or paused node's persisted state must still be ALIVE, otherwise the step fails as "inconclusive, rerun". The get must then log a failover from that node. A pass always means failover was really exercised; liveness-ordered reads are covered by `stage6-liveness-check.sh` and `TestDeadReplicasAreReadLast`. Accepting either path was rejected, because a pass could skip the failover the step exists to test.

**Over:** shortening the coordinator's thresholds for scripts (it would no longer test the shipped configuration); letting steps race detection unchecked (flaky).
**Because:** a check that sometimes tests something else is worse than none.
**Breaks if:** detection is much faster than an initiate. A SUSPECT threshold below 1 s would shrink step 3's window, and its precondition would then fail loudly rather than pass wrongly.

## Mutation scripts extended for stage 6 (stage 6)
**Extends:** "Mutation scripts extended for stage 5" (stage 5).
**Chose:**
- **Coordinator:**
  - L1 no SUSPECT/DEAD transition;
  - L2 placement includes non-ALIVE nodes;
  - L3 a beat from an unknown node is accepted;
  - L4 one beat brings a DEAD node back;
  - L5 UNKNOWN goes DEAD before a full window;
  - L6 `CompleteUpload` asks DEAD nodes;
  - L7 a conflicting instance is accepted.
- **Go:**
  - H1 no per-beat timeout;
  - H2 the loop exits on error;
  - H3 the loop ignores cancellation;
  - H4 gRPC's default reconnect backoff;
  - R1 the read order ignores liveness.

The heartbeat test's cleanup waits for `Run` with a bound, so H3 fails the test instead of hanging the run.
**Because:** each liveness rule is shown to be load-bearing.
**Breaks if:** code edits stop patches from applying. The scripts fail at `git apply --check`.

## What counts as under-replicated; UNKNOWN pauses repair (stage 7)
**Chose:** a chunk's counted replicas are its HEALTHY rows on nodes that are ALIVE, SUSPECT, or DEAD for less than the repair grace period. BAD rows never count. A chunk with fewer than 3 is under-replicated. A chunk born with 2 replicas (while only 2 nodes were ALIVE) counts 2 and is topped up once a third ALIVE node without a row exists. **While any node is UNKNOWN, a pass does nothing**: the state hasn't been rebuilt after a restart (stage 6: never act on UNKNOWN), and the pass is recorded with `skipped_reason`. A chunk with no HEALTHY replica on an ALIVE node can't be repaired; it's logged at error every pass. Mutations P1 (DEAD counts as healthy) and P4 (act on UNKNOWN).
**Over:** counting SUSPECT replicas as missing (a blip would start copies).
**Because:** repair should respond to real loss, not to noise or to state that isn't known yet.
**Breaks if:** a node is SUSPECT for a long time (flapping without going DEAD). Its replicas keep counting.

## Repair waits a grace period after DEAD (stage 7)
**Amends:** "Liveness lives in memory and is persisted for observability and stage 7; what stage 7 can rely on" (stage 6). Repair uses the tracker's monotonic "DEAD since" (`deadForNanos`).
**Chose:** `COORDINATOR_REPAIR_GRACE`, default 30 s beyond DEAD, so 45 s of silence in total. After a coordinator restart "DEAD since" restarts from zero, which delays repair, never hastens it. Measured: with node2 stopped, A and B were back to 3 HEALTHY replicas 46 s after node2 went DEAD. Mutation P7 (no grace).
**Over:** repairing as soon as a node is DEAD (a `docker compose restart` would copy everything the node held).
**Because:** short outages shouldn't cause copy storms.
**Breaks if:** outages are long and frequent. Repair then always waits out the grace, with data at reduced redundancy meanwhile.

## Only current COMMITTED objects are repaired (stage 7)
**Chose:** the reconciler covers chunks of COMMITTED objects that a key currently points to. Superseded versions and PENDING objects are never repaired. Test: `supersededAndPendingObjectsAreNotRepaired`.
**Over:** repairing every COMMITTED object (copies spent on data that stage 8 deletes).
**Because:** in-flight reads of a superseded version finish within minutes, and a failed one can retry and get the current version.
**Breaks if:** readers hold superseded chunk lists for a long time while nodes fail.

## A periodic reconciler, with event hints and priority (stage 7)
**Chose:** a pass every `COORDINATOR_REPAIR_INTERVAL` (10 s) is the source of truth. Hints start a pass early, debounced to at most one extra pass a second: a replica marked BAD, a commit that recorded fewer than 3 replicas, a node becoming ALIVE. They're Spring events, which also avoids a dependency cycle with `ReplicaHealthService`. Work is ordered by counted replicas, fewest first, then object age, then `chunk_id`. Correctness never depends on a hint arriving. Measured: a BAD replica was repaired 1 s after being marked; a born-with-2 object 2 s after its nodes returned.
**Over:** events only (a missed event becomes a permanent gap); a scan only (repair waits up to a full interval).
**Because:** the scan guarantees repair happens; hints make it prompt.
**Breaks if:** the scan gets expensive. Each pass reads every current chunk and its rows, which is fine at this scale.

## Copies are pushed source to target through WriteChunk; the PushChunk RPC (stage 7)
**Amends:** "Client fan-out writes; per-replica writer in a shared package" (stage 4). The reuse that entry planned for stage 7 is realized here.
**Chose:** the coordinator calls `PushChunk(chunk_id, expected_sha256, target_node_id, target_address)` on a source.
1. The source reads its copy through `store.Read`, holding a read slot (so `NODE_MAX_CONCURRENT_READS` bounds it). That checks the magic, the chunk ID and the data hash.
2. It checks the hash equals the coordinator's record; otherwise `DATA_LOSS` (invariant 5).
3. It streams to the target's `internal_addr` with `internal/chunkwrite`, into the target's **unchanged `WriteChunk`**: frame rules, trailer hash, temp → fsync → link (no-replace) → dir fsync.

Problems with the source's own copy are status errors (`DATA_LOSS`, `NOT_FOUND`, `RESOURCE_EXHAUSTED`). What happened at the target is the outcome: `COPIED`, `TARGET_HAS_CHUNK` or `TARGET_FAILED`. Deadline: `COORDINATOR_COPY_TIMEOUT` (30 s). If the coordinator's connection drops, gRPC cancels the source's handler and the target aborts its temp file. Tests: `TestPushChunk` (verified copy, corrupt or mismatched or missing source, target already has the chunk, identity), the unreachable-target and read-slot tests. Mutations Q1 (ignore the expected hash) and Q2 (no `node_id`).
**Over:** the target pulling from the source (a second write path on the node).
**Because:** the copy goes through the most-tested write code there is, and the coordinator never carries bytes (invariant 6).
**Breaks if:** bandwidth between nodes needs throttling. There's none; only the concurrency caps bound it.

## A repaired replica is recorded only after the target's own StatChunks (stage 7)
**Extends:** "Node responses must identify the responding node" (stage 4).
**Chose:** after `COPIED` or `TARGET_HAS_CHUNK`, the coordinator asks the **target** for the chunk with `StatChunks`, applying the identity rule.
- FOUND with the planned size and the recorded SHA-256 → insert a HEALTHY row (`origin = 'repair'`, or `'repair-found'` after `TARGET_HAS_CHUNK`).
- BAD_HEADER, or FOUND with another size or hash → insert a BAD row.
- MISSING or no answer → record nothing.

The push's answer comes from the source, so it's never enough by itself. Mutation P2 (record on `COPIED`), caught by a source that says `COPIED` while the target holds nothing.
**Over:** recording on the push outcome.
**Because:** invariant 3: facts about the target come only from the target.
**Breaks if:** never in correctness terms. It costs one extra RPC per copy.

## Target selection; resolving ALREADY_EXISTS; recording what repair finds (stage 7)
**Amends:** "No retry and no re-placement within an upload; orphaned bytes are left for stages 7 and 8" (stage 4), "Bad files stay on disk until stages 7 and 8; no quarantine" (stage 5), and "Replica state on chunk_replicas; BAD is terminal" (stage 5).
**Chose:** a target is an ALIVE node with **no row of any state** for the chunk; a BAD row's file still blocks the no-replace link until stage 8. Among those, the least busy in this pass, then by `node_id`. **`TARGET_HAS_CHUNK` is never retried as a copy.** The target's own `StatChunks` decides:
- a valid file becomes a `repair-found` HEALTHY row (for example a copy that finished as the coordinator restarted, or an upload's orphaned replica);
- an invalid file becomes a **BAD row inserted directly**, with no HEALTHY history (reason "invalid file found during repair", `origin = 'repair'`). Later passes then skip that node for the chunk, and stage 8 can find the file.

Mutations P3 (target a BAD-row node) and P8 (push again on `TARGET_HAS_CHUNK`). The P3 and P5 tests use layouts where `node_id` tie-breaking alone can't hide the mutation.
**Over:** retrying `ALREADY_EXISTS` (it never succeeds while the file is there); ignoring found files (valid copies wasted, invalid ones targeted forever).
**Because:** every outcome ends in a recorded fact, so the next pass doesn't repeat it.
**Breaks if:** a node holds a valid file whose chunk ID is reused. It can't be: IDs are never reused (invariant 4).

## Source selection; a source's DATA_LOSS goes through the stage 5 confirmation path (stage 7)
**Chose:** sources are HEALTHY rows on ALIVE nodes, least busy first, at most 3 attempts per copy.
- **`DATA_LOSS` or `NOT_FOUND`** from a source → `ReplicaHealthService.reportBadReplica`: `VerifyChunk` on that source, marked BAD only if it confirms. Then the next source.
- **`RESOURCE_EXHAUSTED`, `UNAVAILABLE` or a deadline** → the next source, no report.

Never a BAD replica, never a non-ALIVE node. Test: `aSourceWithDataLossIsConfirmedAndMarkedAndTheNextSourceIsUsed`. Mutation P5 (copy from a BAD replica).
**Over:** marking a source BAD on its own error (the rule is the same as for clients: the node must confirm).
**Because:** a source that fails might simply be busy; one that's corrupt must be confirmed before it's marked.
**Breaks if:** every source is busy for a whole pass. The chunk then waits for the next one.

## Over-replication is left in place; no audit on return (stage 7)
**Chose:** when a DEAD node returns, its old HEALTHY rows count again. Chunks repaired meanwhile then have 4 HEALTHY replicas, and they're left that way: nothing is deleted or marked. Measured: every chunk node2 held had 4 HEALTHY replicas after it returned, and none were BAD. A returning node's rows aren't checked either: that would be a block report, which CLAUDE.md puts out of scope (silent disk loss is detected only at read time). A lost replica is found by a read's `NOT_FOUND`, confirmed through stage 5, then repaired.
**Over:** deleting the surplus now (destructive, on the data just repaired; stage 8 owns deletion); marking it for stage 8 (close to rebalancing, which is out of scope).
**Because:** an extra copy costs disk, not correctness.
**Breaks if:** a node returns with a wiped disk. Its rows overstate until reads find out, and that's the documented limit.

## Concurrency caps; passes don't overlap; work is recomputed each pass (stage 7)
**Chose:** at most `COORDINATOR_REPAIR_MAX_INFLIGHT` (8) copies cluster-wide and `COORDINATOR_REPAIR_MAX_PER_NODE` (2) per node, counting a node as source or target. **The caps bound copies in flight, not copies per pass:** a pass refills freed slots as copies finish, in priority order, and ends when nothing is in flight and nothing more fits. Passes never overlap: one scheduler thread, `runPass` is synchronized, and a pass waits for its copies, so each pass sees every earlier outcome. A pass right after a complete one starts no copies. There's no task table: each pass recomputes from Postgres and liveness (invariant 7). After a coordinator restart, in-flight copies are cancelled (the target aborts its temp), or have already linked a file without a row, which the next pass records as `repair-found` or works around.

The first build applied the caps as a **per-pass budget**: with 3 live nodes and 2 slots each, only 3 copies fit per pass, so 24 chunks took about 80 s across eight passes. The stage 7 script exposed it. `copiesAreBoundedInFlightAndOnePassRepairsEverything` now requires one pass to repair every chunk within the caps. Mutations P6 (no bound) and P10 (per-pass budget).
**Over:** a `repair_tasks` table (state to keep consistent, needed only for overlapping passes or multiple coordinators, both out of scope).
**Because:** bounded and idempotent (invariant 8), with throughput limited only by the caps.
**Breaks if:** a copy hangs: one stuck copy holds its pass open until the copy timeout. Measured: when node4 was killed mid-copy, the pass took 20.1 s, as 25 of its 29 copies failed with connection errors against the dead or restarting node; the next pass completed the repair in 0.2 s.

## V6: replica origin and repair_passes, pruned by retention (stage 7)
**Chose:**
- `chunk_replicas.origin` is `'upload'`, `'repair'` or `'repair-found'`.
- `repair_passes` holds one row per pass: start and end, skip reason, under-replicated count, copies started, rows recorded, found, BAD found, failures.
- At the start of each pass, rows older than `COORDINATOR_REPAIR_PASS_RETENTION` (24 h) are deleted. Test: `oldRepairPassRowsArePrunedAndRecentOnesKept`. Mutation P9.

**Over:** relying on logs (scripts would grep logs for pass outcomes).
**Because:** acceptance checks and people can see exactly what each pass did.
**Breaks if:** passes run far more often than every 10 s. The table is then bounded only by the retention period.

## Earlier scripts compare upload-origin rows; the stage 7 acceptance script (stage 7)
**Amends:** "Scripts are deterministic again under liveness" (stage 6).
**Chose:** background repair only **adds** rows, so earlier exact-replica checks compare `origin = 'upload'` rows:
- `stage4-replication-check.sh`: `check_replicas`, and the two "9 replica rows" counts;
- `stage3-crash-check.sh`: the "node1 recorded from the killed chunk on" count;
- `stage6-liveness-check.sh`: the three replica-set queries.

`stage5-failover-check.sh` needed no change (its checks name specific chunk-and-node states).

Separately, a bug in `stage4-replication-check.sh` step 4 (from stage 6) showed up in the stage 7 verification run: the per-cycle attempt counter restarted each cycle, so cycle 2 reused cycle 1's keys. A genuine failure branch then still resolved to cycle 1's success and went uncounted. Keys now include the cycle (`one-chunk-c<cycle>-<attempt>`); a scratch run forcing every attempt into cycle 2 counted all its failures correctly.

`scripts/stage7-repair-check.sh` covers: a node lost, then repaired and verified on each volume; a repeat pass making no copies; the node returning (4 HEALTHY, none BAD); a BAD replica repaired onto another node; a chunk born with 2 topped up; a target killed mid-copy. For that last one the watcher uses `inotifyd` on node4's `/data/tmp` and SIGKILLs node4's process from a container in the Docker VM's PID namespace: a node-to-node copy of a 4 MiB chunk takes milliseconds, faster than `docker kill`, and the first attempt with the stage 3 `docker kill` watcher missed. The step still requires a temp file at the kill, otherwise it fails as "inconclusive, rerun". `check_chunk_files` lives in `scripts/lib/volume.sh`.
**Because:** repair now runs in the background, and every check must stay deterministic.
**Breaks if:** a new script asserts an exact replica set without filtering by origin.

## Mutation scripts extended for stage 7 (stage 7)
**Extends:** "Mutation scripts extended for stage 6" (stage 6).
**Chose:**
- **Coordinator:**
  - P1 DEAD counts as healthy;
  - P2 record without the target's confirmation;
  - P3 target a node with a BAD row;
  - P4 act on UNKNOWN;
  - P5 copy from a BAD replica;
  - P6 no concurrency bound;
  - P7 no grace;
  - P8 push again on `TARGET_HAS_CHUNK`;
  - P9 no pruning;
  - P10 caps as a per-pass budget.
- **Go:** Q1 `PushChunk` ignores the expected hash; Q2 `PushChunk` response without `node_id`.

The coordinator script now compiles each mutation first and reports `BUILD FAILED` instead of "detected", as the Go script does. A mutation that doesn't compile would otherwise fail every test and look caught. Patch M2 is regenerated against the changed `completeUpload`.
**Because:** every repair rule is shown to be load-bearing, and a broken patch can't pass as one.
**Breaks if:** code edits stop patches from applying. The scripts fail at `git apply --check`.

## What becomes garbage, and the two grace periods (stage 8)
**Chose:** four kinds of garbage, each found from Postgres:

| Garbage | Found as | Files deleted after |
|---|---|---|
| Object removed by `osctl rm` | `DELETED` (set by `rm` itself) | read grace after `retired_at` |
| Superseded version | `COMMITTED` with no `keys` row → `SUPERSEDED` | read grace after `retired_at` |
| Abandoned upload | `PENDING` older than the pending timeout → `ABANDONED` | at once |
| BAD replica's file | BAD row on a `COMMITTED` object | read grace after `marked_bad_at`, and only with ≥ 2 counted HEALTHY replicas (see the BAD-file entry) |

- **Read grace:** `COORDINATOR_GC_READ_GRACE`, code default 15 min. A reader still reading an old version after the grace gets `NOT_FOUND` from each node, so the get fails loudly. It never returns wrong data.
- **Pending timeout:** `COORDINATOR_GC_PENDING_TIMEOUT`, code default 24 h. The longest legitimate upload, 5 GiB as 1,280 sequential chunks with 3× fan-out, can take hours on a slow link. A timeout that's too short fails a slow upload cleanly (next entry); it never corrupts anything.
- **Compose sets demo values,** 60 s and 120 s, with a comment that production should use the code defaults. The acceptance script would otherwise take hours.

**Over:** one value for both; no grace for superseded objects (in-flight readers break); deleting at overwrite or `rm` time.
**Because:** readers that already hold a chunk list need the old files for a bounded time, and an upload in progress must be told apart from a dead one.
**Breaks if:** a get runs longer than the read grace (it then fails), or a legitimate upload outlasts the pending timeout (it then fails at complete with `FAILED_PRECONDITION`).

## Superseded objects are detected by GC and stamped then (stage 8)
**Supersedes:** "Superseded objects are left fully intact until stage 8" (stage 2), the part that deferred deletion and the timestamp.
**Chose:** each pass finds `COMMITTED` objects that no `keys` row points to. Under the object's row lock it re-checks both conditions, then sets `SUPERSEDED` and `retired_at` = now. The overwrite path doesn't change.
**Over:** capturing the old object ID inside the key upsert (that needs a retry loop; stage 2's reason still holds).
**Because:** a key is never re-pointed to an object it left, so "unreferenced and COMMITTED" stays true once it's true. Detection happens at or after the real supersede time, so the grace can only be longer than intended, never shorter. Mutations: G4 (no grace), G15 (supersede ignores keys).

G15 is caught by `theCurrentVersionIsNeverDeleted`. The composite foreign key also refuses to move a still-referenced object out of `COMMITTED`, so the pass fails with an error rather than silently retiring a current object.
**Breaks if:** a future feature re-points a key to an older object (undelete, versioning). That's out of scope.

## Abandoned uploads: an atomic PENDING → ABANDONED under the row lock; a late complete fails (stage 8)
**Amends:** "Object lifecycle: plan persisted at initiate, PENDING → COMMITTED" (stage 2) and "CompleteUpload is idempotent for an already COMMITTED object" (stage 2).
**Chose:**
- **GC side, in one transaction:** `SELECT … FOR UPDATE` on the object row; re-check that it's still PENDING and older than the timeout; set `ABANDONED` and `retired_at`; create its tombstone deletion tasks.
- **`CompleteUpload` side:** phase 3 takes the same lock. Whichever commits first wins:
  - the complete wins: GC then sees COMMITTED and skips it;
  - GC wins: the complete finds `ABANDONED` and fails with `FAILED_PRECONDITION`. If GC has already removed the row, phase 3 finds none, and that is also `FAILED_PRECONDITION`: only GC deletes object rows, and only retired ones.
- **A retried complete** of an object that did commit returns OK: `COMMITTED`, or since `SUPERSEDED` or `DELETED`. This is stage 2's retry rule, extended. It never moves a key back.
- **Late chunk writes** from the still-running client hit the node's tombstone (later entry).

Tests:
- `aLateCompleteOfAnAbandonedUploadFailsAndNothingCommits`: the complete is held after phase 2, with every node's answer FOUND, while a GC pass runs. Mutation G3: GC deletes without the transition.
- `aRetriedCompleteSucceedsForSupersededAndDeletedAndFailsForAbandoned`. Mutation G10: SUPERSEDED fails.

**Over:** checking age and deleting without the lock (a late complete could commit an object whose files are gone); waiting for writes to drain (a live client can keep writing for hours).
**Because:** invariant 2. An object whose chunks GC is deleting can never become COMMITTED.
**Breaks if:** a client's clock-free retry loop keeps a 24 h+ upload going. It then fails at the end, after doing all the work.

## Metadata first; durable deletion tasks; rows removed only on the node's own confirmation (stage 8)
**Chose:**
1. **Metadata first:** the object becomes unreachable (`DELETED`, `SUPERSEDED`, `ABANDONED`) in one transaction.
2. **Tasks:** after the grace, `pending_deletions (chunk_id, node_id, tombstone, reason, attempts, last_error)` gets one row for every (chunk, node) in the object's placements or replica rows. This happens under the row lock, sets `gc_started_at`, and is idempotent.
3. **Deletes:** tasks go to ALIVE nodes in `DeleteChunks` calls. For each chunk the node, answering as itself, reports `DELETED` or `ALREADY_ABSENT`; one transaction then removes the task, that node's replica row and that node's placement.
   - A node that fails, misses the deadline or answers as another node keeps its rows and tasks, with `attempts + 1` and `last_error`.
   - A node that isn't ALIVE isn't called; its tasks wait and are retried every pass once it's ALIVE.
4. **Finish:** a retired object with no task, placement or replica left loses its `chunks` and `objects` rows.

A coordinator restart mid-GC loses nothing: states and tasks are rows (invariant 7). Like repair, a pass does nothing while any node is UNKNOWN.

Tests:
- `rowsAreRemovedOnlyWhenTheNodeItselfConfirms`. Mutation G2: rows removed for a failed or impostor answer.
- `aDeadNodesDeletionsWaitAndAreRetriedWhenItReturns`. Mutation G7: drop a DEAD node's tasks.
- `aSecondPassIsANoOp`. Mutation G14: a task kept after confirmation.

**Over:** deleting files synchronously in `rm` (breaks readers, and fails whenever a node is down); removing rows when the delete is sent (an undelivered delete leaves a file no row points to); recomputing work each pass with no task table, as repair does (the file's location must be kept until the node confirms, and after confirmation the placement that recorded it is gone).
**Because:** invariant 3. "This node no longer holds this chunk" is a fact about the node, so only the node may establish it. A node that returns later is asked again because its rows were never removed.
**Breaks if:** a node is permanently lost. Its tasks then wait forever, and the object's metadata is never removed (bounded rows, no data risk). Decommissioning a node is out of scope.

## Tombstones on the node; a check after the link; a sweep at startup (stage 8)
**Amends:** "Node write path: temp → fsync → link (no-replace) → fsync dir → unlink; temps cleaned before listening" (stage 3).
**Chose:**
- **The tombstone:** `DeleteChunks` with `tombstone` set creates `/data/tombstones/<chunk_id>` and fsyncs that directory **before** unlinking any chunk file.
- **Writes:** `NewWriter` refuses a tombstoned ID (`ErrDeleted` → `FAILED_PRECONDITION`). That's only a fast path. `Commit` checks again **after the link and its directory fsync**: if the tombstone now exists, it unlinks the new file, fsyncs, and fails.

  Either interleaving of a write and a delete ends with no file. Either the write's check sees the tombstone and removes the file, or the tombstone came after the check, so after the link, and the delete's unlink removes it.
- **`StatChunks`** reports a tombstoned chunk `MISSING`.
- **At startup**, before listening and together with the temp cleanup, the node deletes every final chunk file that has a tombstone, fsyncs, and logs each one. Such a file can be left only by a crash between a racing write's link and its check, or between a delete's tombstone and its unlink.
- **Tombstones are empty files, kept forever.**

Tests: `TestWriteRacingDelete` runs a whole delete between a write's fast path and its link (mutation T3: no check after the link); `TestDeleteOrder` (T5: unlink before the tombstone is durable); `TestRecoverStore` (T4: no sweep at startup); `TestDelete`.
**Over:** no tombstones (late uploads and late repair copies leave files that only a listing would find); the node asking the coordinator before each write (puts the coordinator on the write path).
**Because:** invariant 4, enforceable on the node: a deleted chunk ID can never be written again, so GC never chases a file that reappears.
**Breaks if:** tombstones must be pruned (one inode per deleted chunk, forever). Pruning would need to know no write of the ID can still arrive. Out of scope.

## The DeleteChunks RPC: batched, idempotent, addressed to one node, answered by it (stage 8)
**Chose:** `DeleteChunks { node_id, repeated ChunkDeletion { chunk_id, tombstone } }` returns `{ node_id, repeated ChunkDeletionResult { chunk_id, DELETED | ALREADY_ABSENT } }`.
- **Idempotent:** a missing file counts as removed.
- **Validation:** every ID is checked before anything changes, and the request is capped at 4096 chunks.
- **Durability:** one fsync of the tombstones directory and one of the chunks directory per call.
- **Reads:** deleting unlinks the name and never modifies the file, so a read that already opened it gets every byte (`TestOpenReadSurvivesDelete`).
- **Addressing:** the request names the node it's meant for, and a node with another ID refuses the whole request (`FAILED_PRECONDITION`) before touching anything. I added this mid-stage. The design only had the response's `node_id`, which stops the coordinator from **recording** a wrong node's answer, but by then that node has already deleted its own files. For a BAD-file delete (no tombstone, live chunk) those could be the only healthy copies on that node.

Tests: `TestDeleteChunks` (mutations T1: a missing file is an error; T2: no `node_id` in the response; T6: the addressed `node_id` is ignored).
**Over:** one RPC per chunk (1,280 calls for a 5 GiB object); relying only on the response's identity.
**Because:** invariant 3 in both directions. A stale address can neither delete another node's data nor have its answer credited.
**Breaks if:** a node's ID is reassigned to a different disk. That's a configuration error the coordinator can't see.

## Which orphans are cleaned: only those reachable from metadata; repair records its target first (stage 8)
**Amends:** "No retry and no re-placement within an upload; orphaned bytes are left for stages 7 and 8" (stage 4) and "Copies are pushed source to target through WriteChunk; the PushChunk RPC" (stage 7).
**Chose:** GC deletes from every (chunk, node) in `chunk_placements` or `chunk_replicas` of a retired object. `chunk_placements.origin` is `'initiate'` or `'repair'`, and repair inserts its target as a placement **before** ordering the copy. That makes reachable:
- writes that reached a node but were never recorded (a slow replica, a node down at commit);
- losers of concurrent puts (they are superseded objects);
- the abandoned uploads' files;
- copies to a target that vanished mid-copy.

`plannedChunks` and every earlier script that compares against "the placed nodes" now read only `origin = 'initiate'`.

**Explicitly not cleaned:**
- **Files of live objects on a placed node with no row.** At most one per placed node per chunk; repair may adopt one as `repair-found`, and GC deletes it once the object is retired. Stage 4 said stage 8 "must treat 'file present, no replica row' as an orphan". This entry reverses that for live objects: deleting them would race repair's `ALREADY_EXISTS` resolution. The acceptance check counts them through placements.
- **Files with no metadata at all** (an unknown chunk ID, a wiped database). Only listing every node's files finds those, and that is the block report CLAUDE.md puts out of scope.

**Over:** listing node files (block reports).
**Because:** every file the system ever wrote on purpose is recorded somewhere before it's written.
**Breaks if:** some future write path writes a chunk without recording a placement first.

## Repair and GC never cross (stage 8)
**Amends:** "A repaired replica is recorded only after the target's own StatChunks" (stage 7) and "Only current COMMITTED objects are repaired" (stage 7).
**Chose:** `lockIfCurrent` takes a `FOR SHARE` lock on the object row and requires `COMMITTED` and a `keys` row pointing to it. Repair calls it:
- before inserting the target placement (no copy is started for a retired object);
- before recording the confirmed HEALTHY row, or a BAD row found.

Retiring an object (`rm`, or supersede) updates that row, so it waits for the share lock. A lock taken afterwards sees the new status.

| Race | Closed by |
|---|---|
| A pass loaded the chunk, then the key was removed | the placement step finds it not current; no copy |
| A copy in flight when the key is removed | no row recorded; the placement was committed before GC's task creation could run (that needs the row lock and the grace), so GC deletes the copy; a copy arriving after the delete hits the tombstone |
| GC deleting what repair is about to make current | retired objects never become current again; on live chunks GC deletes only BAD files, and repair never targets a node with a BAD row until GC removed that row after the node confirmed |

Test: `repairNeverRecordsACopyForARetiredObjectAndGcFindsTheCopy` (a 500 ms push, `rm` mid-copy). Mutations G6 (record without the check) and G9 (copy without the placement).
**Over:** one thread for both (it removes only some races, not those with clients, and couples their schedules); a global maintenance lock (coarse, and it would stall uploads).
**Because:** repair must never copy, or record, for an object being deleted, and GC must never delete what repair keeps.
**Breaks if:** an object could return to current (see the supersede entry).

## BAD files: after the read grace, only with 2 counted HEALTHY replicas, without a tombstone (stage 8)
**Amends:** "Bad files stay on disk until stages 7 and 8; no quarantine" (stage 5), "Replica state on chunk_replicas; BAD is terminal" (stage 5) and "Target selection; resolving ALREADY_EXISTS; recording what repair finds" (stage 7).
**Chose:** a BAD replica of a `COMMITTED` object gets a deletion task with `tombstone = false` when both of these hold:
- **It was marked longer than the read grace ago.** You chose this mid-stage. The approved design had no grace, but a 10 s pass would remove BAD rows before the stage 5 and 7 scripts check them, and it makes the evidence visible.
- **At least 2 HEALTHY replicas count by repair's rule:** ALIVE, SUSPECT, or DEAD for less than the repair grace. The ≥ 2 condition was also your choice mid-stage, and so was counting with repair's rule: a HEALTHY row on a node DEAD for an hour doesn't count. `RepairService.counts` is shared, so the two can't drift apart. With fewer, the BAD file stays as evidence, and `GetObject`'s `DATA_LOSS` keeps its reasons. Repair tops the chunk up elsewhere first, and then the BAD file can go.

Once the node confirms, the BAD row and that node's placement are removed. The node is then a valid repair target again, and no tombstone stands in the way. This is how stage 5's "stage 8 finds BAD rows, deletes the file, then the row" is done. Within the read grace, a BAD row still blocks the node as a target, as stage 7 said.

Tests:
- `aBadFileIsDeletedWithoutATombstoneAndTheNodeIsATargetAgain`. Mutations G8 (tombstone) and G12 (no grace).
- `aBadFileIsKeptUnlessTwoCountedHealthyReplicasRemain`. Mutation G11: count raw HEALTHY rows.
- `anAllBadChunkKeepsItsFiles`.

The ≥ 2 check runs when the task is created. A HEALTHY replica marked BAD between task creation and the delete isn't re-counted.
**Over:** immediate deletion (as first designed); a tombstone (the node could never get a good copy back); always deleting (all-BAD chunks would lose their last copies and the recorded reasons).
**Because:** a BAD file is useless as data but useful as evidence. It is removed only when the chunk no longer depends on it.
**Breaks if:** `marked_bad_at` (database clock) and the coordinator clock differ by much. The grace is then off by that much (next entry).

## One wall clock for GC's timestamps (stage 8)
**Chose:** a `java.time.Clock` bean, `Clock.systemUTC()` in production, provides now for GC's cutoffs, `retired_at`, `gc_started_at`, and the `created_at` of new uploads (initiate now passes it explicitly instead of the column default). Tests replace it with a settable clock. Liveness keeps its monotonic `Ticker`, because liveness intervals are never stored. `marked_bad_at` stays on the database clock (stage 5's `clock_timestamp()`).
**Over:** `clock_timestamp()` everywhere (grace periods couldn't be tested without sleeping); the `Ticker` (monotonic nanos can't be stored or compared across restarts).
**Because:** a pending timeout and a read grace both compare a stored instant with now, and they must use the same clock to mean anything.
**Breaks if:** the coordinator's and Postgres's clocks drift apart by a large fraction of the read grace. Only the BAD-file grace mixes the two.

## Over-replication is still left in place (stage 8)
**Confirms:** "Over-replication is left in place; no audit on return" (stage 7).
**Chose:** GC never deletes copies of live data. A 4th HEALTHY copy appears only when a node that held the chunk returns after repair, so with 4 nodes it's at most 4 copies per chunk: at most 33 % extra disk.
**Over:** trimming after confirming 3 other HEALTHY copies with `VerifyChunk` (the riskiest delete in the system, done on live data, for a bounded cost; close to rebalancing).
**Because:** an extra copy costs disk, not correctness.
**Breaks if:** nodes come and go often enough that the extra disk matters.

## rm: the key row first, last commit wins; the foreign key unchanged (stage 8)
**Amends:** "A key can only point to a COMMITTED object, enforced by a composite foreign key" (stage 2).
**Chose:**
- **`DeleteObject(key)`, in one transaction:** `DELETE FROM keys … RETURNING object_id` (taking the key row lock), then `COMMITTED → DELETED` with `retired_at`.
- **A key that doesn't resolve:** `NOT_FOUND`, and `osctl rm` exits 1 with "key not found".
- **`rm` racing an overwrite of the same key** serializes on the key row; the last to commit wins:
  - `rm` first: the old version becomes DELETED, then the put points the key at the new one;
  - put first: `rm` removes the new version, and the old one is later found SUPERSEDED.
- **Foreign key:** unchanged. Only the `CHECK` on `objects.status` widens, to `SUPERSEDED`, `DELETED` and `ABANDONED`. Stage 2 warned the FK would need changing "if a later stage adds an object status that must stay readable". These three must not stay readable, so it holds. It already forces the order `rm` uses: the key row must go before the object can leave COMMITTED.

Tests: `rmMakesTheKeyNotFoundAtOnceAndItsFilesGoAfterTheGrace` and `rmRacingAnOverwriteEndsInALastCommitWinsState` (10 rounds, either outcome must be consistent). Neither has a dedicated mutation: the FK and the key row lock make the obvious mistakes fail loudly.
**Over:** idempotent success for a missing key (S3's choice; you chose `NOT_FOUND`); a `DELETING` status that keeps the key (the key would have to stay readable, which is the case stage 2 warned about).
**Because:** `rm` is the same atomic pointer change as an overwrite, pointing at nothing.
**Breaks if:** clients need conditional deletes ("delete only if still version X"). That needs compare-and-swap on `keys`.

## osctl ls and stat (stage 8)
**Chose:**
- **`ListObjects(prefix)`** returns the keys that currently resolve, in key order, with size, `committed_at` and object ID. `osctl ls [prefix]` prints them as a table.
- **`StatObject(key)`** returns the object and every replica row of every chunk, in any state, with origin, BAD reason and the node's liveness. `osctl stat <key>` prints it.

Both are read-only, one statement each. A missing key for `stat` is `NOT_FOUND`.
**Over:** leaving them for later. CLAUDE.md lists them as CLI commands but attaches them to no stage, and stage 9's demo needs "show final replica state".
**Because:** you approved adding them in this stage.
**Breaks if:** there are many keys: `ls` isn't paginated.

## GC bounds, gc_passes, and V7 (stage 8)
**Chose:**
- **Passes:** one every `COORDINATOR_GC_INTERVAL` (10 s); passes never overlap (`runPass` is synchronized) and run alongside repair.
- **Per step:** at most `COORDINATOR_GC_BATCH` (256) objects are retired or scheduled.
- **`DeleteChunks` calls:** at most 256 IDs each, at most `COORDINATOR_GC_MAX_PER_NODE` (1) in flight per node and `COORDINATOR_GC_MAX_INFLIGHT` (4) overall, each within the 2 s node RPC deadline. Rounds repeat within a pass until every ALIVE node's tasks are done; a node that fails is left until the next pass.
- **Every round makes progress, so a pass ends.** A result that is neither `DELETED` nor `ALREADY_ABSENT` (say, `OUTCOME_UNSPECIFIED`) is a failure for that chunk: `attempts + 1`, `last_error`, and the node is left until the next pass. A node whose batch confirms nothing is also left until the next pass. Found when mutation G14 hung the mutation run: the rounds asked for the same tasks forever. Real code could do the same, because an unclear outcome was skipped without marking anything, and the synchronized `runPass` would then have stopped GC for good.
- **`gc_passes`:** one row per pass with counts, pruned after `COORDINATOR_GC_PASS_RETENTION` (24 h).
- **Migration V7:**
  - widens the status CHECK;
  - adds `retired_at` and `gc_started_at`, with a CHECK that retired statuses have `retired_at`, and an index on `(status, created_at)`;
  - adds `chunk_placements.origin`;
  - creates `pending_deletions` and `gc_passes`.

Tests: `deletesAreBatchedAndBounded` (30 deletes with a batch of 4, 1 per node, 2 overall), mutation G13 no overall bound; `anUnclearOutcomeIsAFailureAndThePassEnds` (the node is asked once, its task stays with attempts 1, the other nodes confirm), mutation G16 restores the old loop.
**Over:** one call per chunk; unbounded parallelism; a fixed cap on rounds per pass (an arbitrary number that still repeats a call that achieves nothing).
**Because:** GC must never compete with reads and writes for nodes or the database.
**Breaks if:** deletions arrive faster than about 256 chunks per node per round. The backlog grows, and it shows in `pending_deletions`.

## Earlier scripts adapted to stage 8 (stage 8)
**Amends:** "Earlier scripts compare upload-origin rows; the stage 7 acceptance script" (stage 7).
**Chose:**
- **Placement queries read `origin = 'initiate'`**, because repair targets are placements now:
  - `stage4-replication-check.sh`: `check_replicas`, `placement_width`, `placed_on`, the node3 chunk list, `min_live`, and the two-alive check;
  - `stage6-liveness-check.sh`: "nothing placed on node2";
  - `stage3-fsync-trace.sh`: the node1 placement count.
- **`stage5-failover-check.sh`:** `bad_count` takes the step's key. Steps 3 and 4 compared a cluster-wide BAD count before and after, and GC now deletes earlier steps' BAD rows after the read grace.
- **`flip_byte` refuses a missing file**, because `dd` would create one.
- **`unflip` flips back only a byte that still holds the flipped value.** GC may delete a BAD replica's file and repair write a good copy under the same name, and flipping that copy would corrupt a replica Postgres records HEALTHY. Every copy of a chunk has the same bytes, and a flip XORs with 255, so a good copy never holds the flipped value. Restoring is therefore idempotent: each script restores explicitly before its verdict, and its EXIT trap restores again for early exits.
  - *Corrected in place:* this first recorded each flipped file's inode and unflipped only the same inode. An inode number can't identify a file over time: when a file is deleted, the filesystem frees its inode number and may give it to the next file it creates (ext4 commonly does). A stage 8 run showed exactly that. Node1's BAD file was deleted at 00:12:56, repair wrote a good copy at 00:13:06 under the same inode number, and the exit trap flipped a byte in the good copy. The replica was left HEALTHY in Postgres and corrupt on disk.
- **Every script that flips bytes checks what it touched**, after restoring and before its verdict (`check_touched` in `scripts/lib/volume.sh`). Each touched file must be gone, or, if Postgres has a HEALTHY row for it, pass the header, chunk-ID and SHA-256 checks. Otherwise the script fails, naming the node and chunk. A file whose row is BAD or absent is never served, so it may stay. Its loop feeds `psql` from `/dev/null`: `compose exec` forwards stdin even with `-T`, and on its first run it swallowed the rest of the list, so only the first touched file was checked. Used by `stage3-corrupt-check.sh`, `stage5-failover-check.sh`, `stage7-repair-check.sh` and `stage8-gc-check.sh`.
- **`wait_alive` no longer exits silently while no node is ALIVE.** Its `grep -o '=ALIVE'` exits 1 on zero matches, and under `set -e` with `pipefail` that ended the calling script with no output. The window is right after `up --wait`, while the coordinator is seconds old and every node is still UNKNOWN. Reproduced 2 times in 3 on a coordinator restart; it caused one silent stage 8 exit 1 after 1.4 s. Also, `liveness_states` now prints a failed query's error and returns no states, so callers keep polling to their timeout instead of the error being discarded.
- **`scripts/stage8-gc-check.sh`** covers the six acceptance checks. The overwrite step pauses the node a get reads chunk 0 from, so the overwrite commits while that get holds the old chunk list; the step requires this and otherwise fails as inconclusive. Its first run passed but showed the get finishing before the commit, so it didn't prove the overlap.
  - *Corrected in place:* step 4 first looked for a chunk with no replica on node4 and stopped node4. When every chunk of the 3-chunk object had a replica on node4 (odds (3/4)³ ≈ 42%), the query came back empty and the script died under `set -e` without a verdict. Step 4 now takes chunk 0 and stops the one node without a replica of it. Step 5 assumed node2 held part of its object (it holds none with odds (1/4)³ ≈ 1.6%); it now stops a node that holds chunk 0.
- **Existing ITs** set `coordinator.gc-interval=0`, and their `TRUNCATE`s include `pending_deletions`.

**Because:** background GC now deletes rows and files, and every check must stay deterministic.
**Breaks if:** a new script compares cluster-wide BAD counts, or flips bytes without `corrupt_byte`, `restore_flipped` and `check_touched`; or a chunk's copies stop being byte-identical (they can't while chunks are immutable and IDs never reused).

## Mutation scripts extended for stage 8 (stage 8)
**Extends:** "Mutation scripts extended for stage 7" (stage 7).
**Chose:**
- **Coordinator:**
  - G1 delete before retire;
  - G2 remove the row before the node confirms;
  - G3 abandon without the atomic transition;
  - G4 no grace for superseded objects;
  - G5 delete chunks of the current version (tasks scoped by key, not object);
  - G6 repair records a replica for a retired object;
  - G7 drop a DEAD node's tasks;
  - G8 BAD deletes tombstone;
  - G9 repair copies without the placement;
  - G10 a complete of a SUPERSEDED object fails;
  - G11 count raw HEALTHY rows;
  - G12 no grace for BAD files;
  - G13 no overall delete bound;
  - G14 a task kept after confirmation;
  - G15 supersede ignores keys;
  - G16 no progress check (an unclear outcome is skipped, and a node that confirms nothing is asked again in the same pass).
- **Go:**
  - T1 a missing file is an error;
  - T2 no `node_id` in the response;
  - T3 no tombstone check after the link;
  - T4 no tombstone sweep at startup;
  - T5 unlink before the tombstone is durable;
  - T6 the addressed node is ignored.
- **Regenerated against changed code:** N3 (`Commit` gained the tombstone check), and P1, P3 and P7 (the counting rule moved into `RepairService.counts`).

**Because:** every GC rule is shown to be load-bearing.
**Breaks if:** code edits stop patches from applying. The scripts fail at `git apply --check`.

## Mutation runs: only the target's class per mutation, on the daemon, reverted on any exit (stage 8)
**Amends:** "Atomicity is proven by a fault-injection trigger test; mutations as patches" (stage 2) and the mutation-script entries of stages 3–7, which ran the whole coordinator suite under every mutation.
**Chose:**
- **What "detected" means now:** each coordinator mutation names `Class|test`; the script runs only that class (`--tests`), and the mutation is DETECTED when that test fails in that run. The full unmutated suite still runs once at the end and guards everything else. "Alone" in earlier tables now means alone within the target's class; other classes are not run under the mutation.
- **The Gradle daemon** instead of `--no-daemon`, and `test --rerun` instead of `--rerun-tasks`, so compilation is incremental and only the test task is forced.
- **A default JUnit timeout:** 30 s per test, in its own thread (`junit-platform.properties`), so a hang is a named failure rather than a run that never ends. The slowest test takes under a second.
- **Revert on any exit:** both scripts source `scripts/lib/mutation.sh`, which reverts the applied patch on EXIT, SIGINT and SIGTERM. Test runs go into the background and the script waits for them, so a signal is handled at once: the run's processes are killed and the patch reverted. Before the first patch, each script checks every patch in `scripts/mutations/` (`git apply --check` succeeds, `git apply -R --check` fails) and refuses to start otherwise.

**Over:** the whole suite under every mutation (47 × about 6 min, and one hang stalled it all); a per-mutation wall-clock limit in the script (it would not say which test hung).
**Because:** a mutation targets one rule, and that rule's test is in one class. Nothing is lost on the unmutated side, since that run still covers every class. The kill-safe revert matters because a run interrupted by a kill would otherwise leave a mutation in the tree. A later commit could then ship it.
**Breaks if:** a mutation's only detecting test moves to another class without the `Class|test` entry changing (the script then reports NOT DETECTED, loudly); a mutation would also have broken another class's tests and that mattered (it is no longer seen); or a test legitimately needs more than 30 s and gets its own `@Timeout`.
