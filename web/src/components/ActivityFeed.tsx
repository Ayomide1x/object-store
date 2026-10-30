import type { Cluster } from "../api";
import { feedItems } from "../feed";
import { formatTime } from "../format";

export function ActivityFeed({ cluster }: { cluster: Cluster | null }) {
  const items = cluster === null ? [] : feedItems(cluster);
  return (
    <section className="activity" aria-labelledby="activity-title">
      <h2 id="activity-title">Activity</h2>
      {cluster !== null && cluster.liveness_events_dropped > 0 ? (
        <p className="notice" data-tone="unknown">
          {cluster.liveness_events_dropped} liveness {cluster.liveness_events_dropped === 1 ? "change was" : "changes were"} not
          recorded since the coordinator started.
        </p>
      ) : null}
      {items.length === 0 ? <p className="muted">Nothing has happened yet.</p> : null}
      <ol className="feed" data-testid="feed">
        {items.map((i) => (
          <li key={i.id} data-testid="feed-item" data-kind={i.kind} data-node={i.node} data-from={i.from} data-to={i.to}>
            <time dateTime={new Date(i.at).toISOString()}>{formatTime(i.at)}</time>
            <span>{i.text}</span>
          </li>
        ))}
      </ol>
    </section>
  );
}
