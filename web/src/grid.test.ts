import { describe, expect, it } from "vitest";
import type { NodeStatus, Replica, Stat } from "./api";
import { deriveGrid, landedRepairs } from "./grid";

const node = (node_id: string, liveness: NodeStatus["liveness"] = "ALIVE"): NodeStatus => ({
  node_id,
  liveness,
  liveness_since_unix_millis: 1,
  last_heartbeat_unix_millis: 1,
});

const replica = (node_id: string, more: Partial<Replica> = {}): Replica => ({
  node_id,
  state: "HEALTHY",
  origin: "upload",
  liveness: "ALIVE",
  ...more,
});

function stat(chunks: { replicas: Replica[]; counted: number }[]): Stat {
  return {
    key: "k",
    object_id: "o",
    size_bytes: 10,
    committed_at_unix_millis: 1,
    chunks: chunks.map((c, i) => ({
      chunk_id: `c${i}`,
      index: i,
      size_bytes: 10,
      sha256: "ab",
      replicas: c.replicas,
      counted_replicas: c.counted,
    })),
  };
}

const four = [node("node1"), node("node2", "DEAD"), node("node3"), node("node4")];

describe("deriveGrid", () => {
  it("has a column for every node, sorted, including nodes with no replicas", () => {
    const g = deriveGrid(stat([{ replicas: [replica("node3")], counted: 1 }]), [node("node4"), node("node1"), node("node3")]);
    expect(g.nodes).toEqual(["node1", "node3", "node4"]);
    expect(g.rows[0]?.cells.map((c) => c.state)).toEqual(["ABSENT", "HEALTHY", "ABSENT"]);
  });

  // Mutation u3: repair copies are shown as upload copies.
  it("marks repair copies, and only HEALTHY ones", () => {
    const g = deriveGrid(
      stat([
        {
          replicas: [
            replica("node1"),
            replica("node3", { origin: "repair" }),
            replica("node4", { origin: "repair-found" }),
          ],
          counted: 3,
        },
        { replicas: [replica("node1", { origin: "repair", state: "BAD", bad_reason: "checksum mismatch" })], counted: 0 },
      ]),
      four,
    );
    const [first, second] = g.rows;
    expect(first?.cells.map((c) => c.repairCopy)).toEqual([false, false, true, true]);
    expect(first?.cells[2]?.label).toBe("Chunk 0 on node3: Repair copy");
    expect(second?.cells[0]?.repairCopy).toBe(false);
  });

  // Mutation u4: a BAD replica is shown as healthy.
  it("shows BAD as bad, with its reason, never as healthy", () => {
    const g = deriveGrid(stat([{ replicas: [replica("node1", { state: "BAD", bad_reason: "checksum mismatch" })], counted: 0 }]), four);
    const cell = g.rows[0]?.cells[0];
    expect(cell?.state).toBe("BAD");
    expect(cell?.label).toBe("Chunk 0 on node1: Bad (checksum mismatch)");
  });

  // Mutation u5: absent is rendered like BAD.
  it("tells a node with no copy apart from a bad copy", () => {
    const g = deriveGrid(stat([{ replicas: [replica("node1", { state: "BAD" })], counted: 0 }]), four);
    expect(g.rows[0]?.cells.map((c) => c.state)).toEqual(["BAD", "ABSENT", "ABSENT", "ABSENT"]);
    expect(g.rows[0]?.cells[1]?.label).toBe("Chunk 0 on node2: No copy");
  });

  it("notes a replica on a node that isn't Alive", () => {
    const g = deriveGrid(stat([{ replicas: [replica("node2")], counted: 1 }]), four);
    expect(g.rows[0]?.cells[1]?.label).toBe("Chunk 0 on node2: Healthy (node Dead)");
    expect(g.rows[0]?.cells[1]?.nodeLiveness).toBe("DEAD");
  });

  it("takes the row's status from counted_replicas, not from its own count", () => {
    // Three HEALTHY rows, but one is on a node past the repair grace: the
    // coordinator counts 2.
    const g = deriveGrid(stat([{ replicas: [replica("node1"), replica("node2"), replica("node3")], counted: 2 }]), four);
    expect(g.rows[0]?.counted).toBe(2);
    expect(g.rows[0]?.underReplicated).toBe(true);
    const full = deriveGrid(stat([{ replicas: [replica("node1"), replica("node3"), replica("node4")], counted: 3 }]), four);
    expect(full.rows[0]?.underReplicated).toBe(false);
  });

  it("adds a column for a replica's node the cluster doesn't list", () => {
    const g = deriveGrid(stat([{ replicas: [replica("node9", { liveness: "UNKNOWN" })], counted: 0 }]), [node("node1")]);
    expect(g.nodes).toEqual(["node1", "node9"]);
    expect(g.rows[0]?.cells[1]?.nodeLiveness).toBe("UNKNOWN");
  });
});

describe("landedRepairs", () => {
  it("finds cells that just became repair copies", () => {
    const before = deriveGrid(stat([{ replicas: [replica("node1"), replica("node2")], counted: 2 }]), four);
    const after = deriveGrid(
      stat([{ replicas: [replica("node1"), replica("node2"), replica("node4", { origin: "repair" })], counted: 3 }]),
      four,
    );
    expect([...landedRepairs(before, after)]).toEqual(["c0/node4"]);
    expect(landedRepairs(after, after).size).toBe(0);
    expect(landedRepairs(null, after).size).toBe(0);
  });
});
