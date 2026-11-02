// The placement matrix: one row per chunk, one column per node.
import type { Liveness, NodeStatus, ReplicaOrigin, Stat } from "./api";

/** The replication target (RepairService.TARGET_REPLICAS). */
export const TARGET_REPLICAS = 3;

/** What a cell shows. ABSENT: the node has no row for the chunk. */
export type CellState = "HEALTHY" | "BAD" | "ABSENT";

export interface Cell {
  nodeId: string;
  state: CellState;
  origin: ReplicaOrigin | null;
  /** A HEALTHY copy the reconciler made (origin repair or repair-found). */
  repairCopy: boolean;
  nodeLiveness: Liveness;
  badReason: string;
  /** The words for this cell, for the screen and for assistive technology. */
  label: string;
}

export interface Row {
  index: number;
  chunkId: string;
  sizeBytes: number;
  sha256: string;
  /** Replicas that count toward the target, by the coordinator's own rule. */
  counted: number;
  underReplicated: boolean;
  cells: Cell[];
}

export interface Grid {
  nodes: string[];
  rows: Row[];
}

export const LIVENESS_WORD: Record<Liveness, string> = {
  ALIVE: "Alive",
  SUSPECT: "Suspect",
  DEAD: "Dead",
  UNKNOWN: "Unknown",
};

/** A cell's state in words, without the node: "Healthy, repair copy". */
export function cellWords(cell: Pick<Cell, "state" | "repairCopy" | "badReason">): string {
  switch (cell.state) {
    case "HEALTHY":
      return cell.repairCopy ? "Repair copy" : "Healthy";
    case "BAD":
      return cell.badReason ? `Bad (${cell.badReason})` : "Bad";
    case "ABSENT":
      return "No copy";
  }
}

/**
 * Builds the grid for stat. Columns are every node the cluster knows, then
 * any node a replica names that it doesn't, sorted by ID. Row status comes
 * from the coordinator's counted_replicas, never recounted here: the grace
 * window for a DEAD node is the coordinator's to judge.
 */
export function deriveGrid(stat: Stat, clusterNodes: NodeStatus[]): Grid {
  const liveness = new Map<string, Liveness>(clusterNodes.map((n) => [n.node_id, n.liveness]));
  for (const c of stat.chunks) {
    for (const r of c.replicas) {
      if (!liveness.has(r.node_id)) {
        liveness.set(r.node_id, r.liveness);
      }
    }
  }
  const nodes = [...liveness.keys()].sort();
  const rows = stat.chunks.map((c): Row => {
    const byNode = new Map(c.replicas.map((r) => [r.node_id, r]));
    const cells = nodes.map((nodeId): Cell => {
      const r = byNode.get(nodeId);
      const nodeLiveness = liveness.get(nodeId) ?? "UNKNOWN";
      const state: CellState = r === undefined ? "ABSENT" : r.state;
      const repairCopy = r !== undefined && r.state === "HEALTHY" && r.origin !== "upload";
      const badReason = r?.state === "BAD" ? (r.bad_reason ?? "") : "";
      const words = cellWords({ state, repairCopy, badReason });
      const nodeNote = r !== undefined && nodeLiveness !== "ALIVE" ? ` (node ${LIVENESS_WORD[nodeLiveness]})` : "";
      return {
        nodeId,
        state,
        origin: r?.origin ?? null,
        repairCopy,
        nodeLiveness,
        badReason,
        label: `Chunk ${c.index} on ${nodeId}: ${words}${nodeNote}`,
      };
    });
    return {
      index: c.index,
      chunkId: c.chunk_id,
      sizeBytes: c.size_bytes,
      sha256: c.sha256,
      counted: c.counted_replicas,
      underReplicated: c.counted_replicas < TARGET_REPLICAS,
      cells,
    };
  });
  return { nodes, rows };
}

/** Cells that became a HEALTHY repair copy since prev: "chunkId/nodeId". */
export function landedRepairs(prev: Grid | null, next: Grid): Set<string> {
  const out = new Set<string>();
  if (prev === null) {
    return out;
  }
  const before = new Map<string, Cell>();
  for (const row of prev.rows) {
    for (const cell of row.cells) {
      before.set(`${row.chunkId}/${cell.nodeId}`, cell);
    }
  }
  for (const row of next.rows) {
    for (const cell of row.cells) {
      const id = `${row.chunkId}/${cell.nodeId}`;
      const was = before.get(id);
      if (cell.repairCopy && was !== undefined && !was.repairCopy) {
        out.add(id);
      }
    }
  }
  return out;
}

/**
 * What the node card says about the coordinator's own calls to a node, or
 * null when they are going fine. Heartbeats come from the node, calls go to
 * it: a node can be Alive and still unreachable from the coordinator, and
 * that must not hide behind "Alive".
 */
export function reachabilityNote(node: Pick<NodeStatus, "liveness" | "recent_call_failures" | "channel_replacements">): string | null {
  const f = node.recent_call_failures;
  if (f === 0) {
    return null;
  }
  const calls = `${f} failed ${f === 1 ? "call" : "calls"} from the coordinator in the last minute`;
  const replaced = node.channel_replacements > 0 ? `; reconnected ${node.channel_replacements} ${node.channel_replacements === 1 ? "time" : "times"}` : "";
  return node.liveness === "ALIVE" ? `Alive, but unreachable: ${calls}${replaced}` : `${calls}${replaced}`;
}
