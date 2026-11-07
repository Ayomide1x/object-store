# How it's tested

The test layers, what each acceptance script proves, how a script reports its verdict, and how the mutation checks work. The scripts live in [`scripts/`](../scripts/); run them against the stack from the [quick start](../README.md#quick-start).

| Layer | What | Run |
|---|---|---|
| Go unit tests | node chunk store (durability order, tombstones, crash recovery), node RPCs, heartbeats (including the redial after a half-open connection), client upload and failover; keepalive on the long-lived connections and the node's ping policy; gateway handlers against a fake coordinator and fake nodes (411/413, key encoding, the error mapping, the caps, streaming both ways, the mid-stream abort, disconnects and stalls, the Host and cross-origin checks, Content-Disposition, the embedded UI's headers, the health probe) | `go test ./...` |
| Coordinator integration tests | 129 tests against a real Postgres (Testcontainers) and in-process fake nodes: the commit protocol, bad-replica reports, liveness, repair, GC, listing pagination, the node address set, cluster status and its cache, per-chunk counted replicas, the liveness event log (including a failing and a hanging event store), node channels that get stuck after an address change (real gRPC servers, a DNS the test controls), half-open node connections closed by keepalive, and the server's ping policy | `cd coordinator && ./gradlew test` |
| Web unit tests | Vitest: key encoding, the grid derivation, upload messages, the poller (no overlap, paused when hidden, backoff), the feed, keys rendered as text; plus lint rules that keep keys out of markup and object bytes out of JavaScript | `cd web && npm test && npm run lint` |
| Browser tests | Playwright against the real stack: the UI follows `demo.sh` (Suspect, Dead, repair, Bad, repaired), browser and `osctl` round trips both ways, paging and delete, a corrupted replica, a broken download | `scripts/stage11-ui-check.sh` |
| Acceptance scripts | one per stage, against the real Compose stack; each checks what actually happened on disk and in Postgres, and fails as inconclusive rather than passing on a run that didn't test anything | below |
| Mutation checks | each patch in `scripts/mutations/` breaks one rule; the named test (or lint rule) must fail | `scripts/go-mutation-check.sh`, `scripts/coordinator-mutation-check.sh`, `scripts/web-mutation-check.sh` |

| Script | Proves |
|---|---|
| `stage2-roundtrip.sh`, `stage2-overwrite-check.sh` | 0 B / 1 B / 4 MiB / 100 MiB round trips; a get during overwrites sees the old or the new object, never a mix |
| `stage3-corrupt-check.sh`, `stage3-crash-check.sh`, `stage3-fsync-trace.sh` | a flipped byte fails the get loudly; a node killed mid-write leaves no partial file; the fsync order, traced |
| `stage4-replication-check.sh` | 3 replicas per chunk; uploads with a node stopped or hung; clean failure with too few nodes; after a forced IP swap between two nodes, uploads, repair and GC reach every node; half-open connections (one connection's packets dropped, nothing closed) are replaced: coordinator to node within 12 s, a node's heartbeat without it going DEAD, gateway to coordinator |
| `stage5-failover-check.sh` | reads fail over and the bad replica is marked, only on its node's word |
| `stage6-liveness-check.sh` | ALIVE → SUSPECT → DEAD → ALIVE within the configured windows |
| `stage7-repair-check.sh` | every chunk of a stopped node gets back to 3 replicas; a target killed mid-copy leaves nothing partial |
| `stage8-gc-check.sh` | after a client is killed mid-upload, no orphan chunk files remain; rm, overwrite and GC with a node down |
| `stage10-gateway-check.sh` | curl round trips; 411 and 413; a later chunk corrupt on every replica gives a broken transfer with exactly the verified prefix; two gateways and a restart; overwrite, rm, ls and stat match `osctl` in both directions |
| `stage11-ui-check.sh` | on a fresh stack, the browser suite above; a foreign Host gets 403 on every route and a cross-origin DELETE deletes nothing; both gateways healthy with the allowlist set |
| `demo.sh` | the demo story, end to end |
| `caddy-check.sh` | (deployment, local) with the VM's Caddy and site block in front of the production stack: basic auth on every route and a wrong password's cost; Host and Origin passed through; `%2F` kept encoded; 413; a broken download fails over HTTP/1.1, HTTP/2 and HTTP/3; a 512 MiB upload streams |
| `deploy-smoke.sh` | (deployment, from your machine) against the live URL: 401 without the password, `/cluster`, a round trip under a key with "/", a broken download over HTTP/2 and in your browser |

Each script ends PASS (exit 0), FAIL (exit 1: the product got a check wrong) or INCONCLUSIVE (exit 75: nothing failed, but a precondition or a random draw didn't happen, so rerun); CI reruns an inconclusive script up to 3 times. Scripts that corrupt files restore them afterwards and check that every file they touched is gone or verifies. A mutation counts as detected when its named test fails (for the coordinator, in a run of only that test's class); each script then runs the full unmutated suite once. CI runs the Go, coordinator and web unit tests on every push, and the acceptance scripts and the three mutation checks weekly.

## The demo

```sh
docker compose down -v && docker compose up --build --wait
make build
scripts/demo.sh
```

About two minutes, on the shipped settings. It uploads a 40 MiB file, stops a node (SUSPECT, then DEAD), waits out the repair grace and shows each chunk being copied, starts the node again, corrupts one replica on disk and downloads the file intact anyway (the replica is marked `BAD` and the chunk repaired), and ends with `osctl stat`.

The demo is also an acceptance test: every step checks that what it shows happened and stops with `FAIL` and the reason if not. `DEMO_PAUSE=5 scripts/demo.sh` holds 5 s after each step; with the UI open on `/?key=demo%2Fphoto-archive.bin` you can watch it happen. `scripts/record-demo.sh` records it (needs `asciinema` 3 and `agg`).
