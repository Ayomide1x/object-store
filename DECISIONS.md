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
