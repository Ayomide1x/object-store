# Architecture: the life of a put, a get, and the background work

The full walkthrough behind the [README's architecture diagram](../README.md#architecture): what happens on an upload, on a download, and in the background. The reasons for each choice are in [DECISIONS.md](../DECISIONS.md).

**Life of a put**
1. `osctl put` asks the coordinator to initiate an upload. The coordinator creates a `PENDING` object and plans its chunks: an ID and 3 ALIVE nodes each.
2. The client streams each chunk to its 3 nodes in parallel, in frames of at most 1 MiB, with the chunk's SHA-256.
3. Each node verifies size and hash, writes a temp file, fsyncs it, links it to its final name (never replacing anything) and fsyncs the directory.
4. The client calls CompleteUpload. The coordinator asks each node what it holds (`StatChunks`, answered under the node's own ID), records only the replicas that check out, and commits only if every chunk has at least 2.
5. Commit swaps the key's pointer to the new object in the same transaction. Readers see the old version or the new one, never a mix.

**Life of a get**
1. `osctl get` asks the coordinator for the chunk list: each chunk's hash and its healthy replicas, with each node's liveness.
2. For each chunk the client reads one replica (ALIVE nodes first). The node verifies the chunk before sending a byte, and the client checks the received bytes against the recorded hash.
3. On an error, a timeout or a hash mismatch, the client fails over to the next replica. A bad copy is reported; the coordinator asks that node to verify its own copy and marks it `BAD` only if the node confirms.

**In the background**
- **Liveness:** nodes heartbeat every 2 s. After 6 s of silence a node is `SUSPECT`, after 15 s `DEAD`; it needs 2 consecutive beats to come back.
- **Repair:** a reconciler finds chunks with fewer than 3 healthy replicas (a `DEAD` node's copies stop counting 30 s after it died) and orders a node-to-node copy from a verified replica. It is idempotent and bounded.
- **Garbage collection:** `rm` removes the key at once; the object's files are deleted after a read grace, and uploads left `PENDING` are abandoned and collected. A row is removed only when the node itself confirms its file is gone.
