// The activity feed: liveness transitions, and repair and cleanup passes
// that did something, newest first.
import type { Cluster } from "./api";
import { LIVENESS_WORD } from "./grid";

export interface FeedItem {
  id: string;
  at: number;
  kind: "liveness" | "repair" | "cleanup";
  text: string;
  node?: string;
  from?: string;
  to?: string;
}

const MAX_ITEMS = 40;

/** "3 copies started, 1 failed": the non-zero counts, each as [n, one, many]. */
function counts(parts: [number, string, string][]): string {
  return parts
    .filter(([n]) => n > 0)
    .map(([n, one, many]) => `${n} ${n === 1 ? one : many}`)
    .join(", ");
}

export function feedItems(c: Cluster): FeedItem[] {
  const items: FeedItem[] = [];
  for (const e of c.recent_liveness_events) {
    items.push({
      id: `l${e.event_id}`,
      at: e.at_unix_millis,
      kind: "liveness",
      node: e.node_id,
      from: e.from,
      to: e.to,
      text: e.from === "" ? `${e.node_id}: Unknown (the coordinator started)` : `${e.node_id}: ${LIVENESS_WORD[e.from]} to ${LIVENESS_WORD[e.to]}`,
    });
  }
  for (const p of c.recent_repair_passes) {
    const did = counts([
      [p.copies_started, "copy started", "copies started"],
      [p.replicas_recorded, "recorded", "recorded"],
      [p.found_recorded, "found already in place", "found already in place"],
      [p.bad_found, "bad file found", "bad files found"],
      [p.failures, "failed", "failed"],
    ]);
    items.push({
      id: `r${p.pass_id}`,
      at: p.started_at_unix_millis,
      kind: "repair",
      text: p.skipped_reason
        ? `Repair skipped: ${p.skipped_reason}`
        : `Repair found ${p.under_replicated} under-replicated ${p.under_replicated === 1 ? "chunk" : "chunks"}${did ? `: ${did}` : ""}`,
    });
  }
  for (const p of c.recent_gc_passes) {
    const did = counts([
      [p.files_deleted, "file deleted", "files deleted"],
      [p.superseded, "old version retired", "old versions retired"],
      [p.abandoned, "abandoned upload cleared", "abandoned uploads cleared"],
      [p.objects_removed, "object removed", "objects removed"],
      [p.failures, "failed", "failed"],
    ]);
    items.push({
      id: `g${p.pass_id}`,
      at: p.started_at_unix_millis,
      kind: "cleanup",
      text: p.skipped_reason ? `Cleanup skipped: ${p.skipped_reason}` : `Cleanup: ${did || "scheduled deletions"}`,
    });
  }
  return items.sort((a, b) => b.at - a.at || b.id.localeCompare(a.id)).slice(0, MAX_ITEMS);
}
