import { describe, expect, it } from "vitest";
import type { Cluster } from "./api";
import { feedItems } from "./feed";

const empty: Cluster = {
  nodes: [],
  under_replicated_chunks: 0,
  pending_deletions: 0,
  counts_as_of_unix_millis: 0,
  last_repair_pass: null,
  last_gc_pass: null,
  recent_repair_passes: [],
  recent_gc_passes: [],
  recent_liveness_events: [],
  liveness_events_dropped: 0,
};

const pass = { finished_at_unix_millis: 0, found_recorded: 0, bad_found: 0, failures: 0 };

describe("feedItems", () => {
  it("merges transitions and passes, newest first, in plain words", () => {
    const items = feedItems({
      ...empty,
      recent_liveness_events: [
        { event_id: 2, node_id: "node2", from: "SUSPECT", to: "DEAD", at_unix_millis: 3000 },
        { event_id: 1, node_id: "node2", from: "", to: "UNKNOWN", at_unix_millis: 1000 },
      ],
      recent_repair_passes: [
        { ...pass, pass_id: 7, started_at_unix_millis: 4000, under_replicated: 1, copies_started: 1, replicas_recorded: 1 },
        { ...pass, pass_id: 6, started_at_unix_millis: 2000, skipped_reason: "nodes UNKNOWN: [node2]", under_replicated: 0, copies_started: 0, replicas_recorded: 0 },
      ],
    });
    expect(items.map((i) => i.text)).toEqual([
      "Repair found 1 under-replicated chunk: 1 copy started, 1 recorded",
      "node2: Suspect to Dead",
      "Repair skipped: nodes UNKNOWN: [node2]",
      "node2: Unknown (the coordinator started)",
    ]);
    expect(items[1]).toMatchObject({ kind: "liveness", node: "node2", from: "SUSPECT", to: "DEAD" });
  });
});
