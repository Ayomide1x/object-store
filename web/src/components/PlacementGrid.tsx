// The placement matrix. Each node is a bay: its column header is the node's
// card, and its cells run down the page under it, so a stopped node reads as
// one hatched column. Every state is a glyph and a word; color only repeats
// what they say. Tests read the data-* attributes.
import { useState } from "react";
import type { Liveness, NodeStatus } from "../api";
import { formatBytes, formatDuration } from "../format";
import { cellWords, LIVENESS_WORD, TARGET_REPLICAS, landedRepairs, type Cell, type Grid } from "../grid";
import { Glyph, type GlyphKind } from "./Glyph";

const LIVENESS_GLYPH: Record<Liveness, GlyphKind> = {
  ALIVE: "alive",
  SUSPECT: "suspect",
  DEAD: "dead",
  UNKNOWN: "unknown",
};

interface Props {
  nodes: NodeStatus[];
  grid: Grid | null;
  /** Shown in the body when there is no grid. */
  empty: React.ReactNode;
  now: number;
  stale: boolean;
}

export function PlacementGrid({ nodes, grid, empty, now, stale }: Props) {
  // Which cells just became repair copies, computed when the grid changes
  // (React's "adjust state while rendering" pattern).
  const [prevGrid, setPrevGrid] = useState<Grid | null>(null);
  const [landed, setLanded] = useState<Set<string>>(new Set());
  if (grid !== prevGrid) {
    setLanded(grid === null ? new Set() : landedRepairs(prevGrid, grid));
    setPrevGrid(grid);
  }
  const byId = new Map(nodes.map((n) => [n.node_id, n]));
  const columns = grid?.nodes ?? nodes.map((n) => n.node_id).sort();

  return (
    <div className="placement" data-stale={stale}>
      <table className="bays">
        <caption className="visually-hidden">
          Where each chunk is stored: rows are chunks, columns are storage nodes.
        </caption>
        <thead>
          <tr>
            <th scope="col" className="row-head">
              <span className="visually-hidden">Chunk</span>
            </th>
            {columns.map((id) => (
              <NodeBay key={id} id={id} node={byId.get(id)} now={now} />
            ))}
            <th scope="col" className="row-status-head">
              Replicas
            </th>
          </tr>
        </thead>
        <tbody>
          {grid === null || grid.rows.length === 0 ? (
            <tr>
              <td colSpan={columns.length + 2} className="bays-empty">
                {empty}
              </td>
            </tr>
          ) : (
            grid.rows.map((row) => (
              <tr key={row.chunkId} data-testid="chunk-row" data-chunk-index={row.index} data-counted={row.counted}>
                <th scope="row" className="row-head">
                  <span className="chunk-index">{row.index}</span>
                  <span className="chunk-size">{formatBytes(row.sizeBytes)}</span>
                </th>
                {row.cells.map((cell) => (
                  <ReplicaCell
                    key={cell.nodeId}
                    cell={cell}
                    landed={landed.has(`${row.chunkId}/${cell.nodeId}`)}
                  />
                ))}
                <td className="row-status" data-under-replicated={row.underReplicated}>
                  {row.underReplicated ? <Glyph kind="warn" /> : null}
                  <span>
                    {row.counted > TARGET_REPLICAS
                      ? `${row.counted} (target ${TARGET_REPLICAS})`
                      : `${row.counted} of ${TARGET_REPLICAS}`}
                  </span>
                  {row.underReplicated ? <span className="row-status-word">under-replicated</span> : null}
                </td>
              </tr>
            ))
          )}
        </tbody>
      </table>
      {grid !== null && grid.rows.length > 0 ? <Legend /> : null}
    </div>
  );
}

function NodeBay({ id, node, now }: { id: string; node: NodeStatus | undefined; now: number }) {
  const liveness: Liveness = node?.liveness ?? "UNKNOWN";
  const since = node && node.liveness_since_unix_millis > 0 ? now - node.liveness_since_unix_millis : null;
  return (
    <th scope="col" className="bay-head" data-testid="node-card" data-node={id} data-state={liveness}>
      <span className="bay-name">{id}</span>
      <span className="bay-liveness">
        <Glyph kind={LIVENESS_GLYPH[liveness]} />
        <span>{LIVENESS_WORD[liveness]}</span>
      </span>
      <span className="bay-since">{since === null ? " " : `for ${formatDuration(since)}`}</span>
    </th>
  );
}

function cellGlyph(cell: Cell): GlyphKind {
  if (cell.state === "BAD") {
    return "bad";
  }
  if (cell.state === "ABSENT") {
    return "absent";
  }
  return cell.repairCopy ? "repair" : "healthy";
}

function ReplicaCell({ cell, landed }: { cell: Cell; landed: boolean }) {
  return (
    <td
      className={landed ? "cell landed" : "cell"}
      data-testid="replica-cell"
      data-node={cell.nodeId}
      data-state={cell.state}
      data-origin={cell.origin ?? ""}
      data-node-liveness={cell.nodeLiveness}
      aria-label={cell.label}
    >
      <Glyph kind={cellGlyph(cell)} />
      <span className="cell-word" aria-hidden="true">
        {cellWords({ ...cell, badReason: "" })}
      </span>
    </td>
  );
}

function Legend() {
  const items: [GlyphKind, string][] = [
    ["healthy", "Healthy"],
    ["repair", "Repair copy"],
    ["bad", "Bad"],
    ["absent", "No copy"],
  ];
  return (
    <ul className="legend" aria-label="Key to the replica states">
      {items.map(([kind, word]) => (
        <li key={kind}>
          <Glyph kind={kind} />
          {word}
        </li>
      ))}
    </ul>
  );
}
