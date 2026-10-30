import type { Cluster } from "../api";
import { formatDuration, formatTime } from "../format";
import type { ThemeChoice } from "../theme";

export interface PollState {
  /** When the last /cluster answer arrived, or null before the first. */
  updatedAt: number | null;
  error: string | null;
  retryInMs: number;
}

interface Props {
  cluster: Cluster | null;
  poll: PollState;
  now: number;
  theme: ThemeChoice;
  onTheme: (t: ThemeChoice) => void;
}

export function Header({ cluster, poll, now, theme, onTheme }: Props) {
  return (
    <header className="masthead">
      <div className="masthead-title">
        <h1>Object store</h1>
        <span className="masthead-host">{window.location.host}</span>
      </div>
      <p className="cluster-sentence" data-testid="cluster-sentence">
        {cluster === null ? "Waiting for the cluster…" : <ClusterSentence cluster={cluster} />}
      </p>
      <p className="poll-status" data-testid="poll-status" data-state={poll.error === null ? "live" : "stale"} role="status">
        {poll.error === null
          ? poll.updatedAt === null
            ? "Connecting…"
            : `Updated ${formatDuration(now - poll.updatedAt)} ago`
          : `${poll.error} Retrying in ${formatDuration(poll.retryInMs)}.`}
      </p>
      <fieldset className="theme-switch">
        <legend>Theme</legend>
        {(["system", "light", "dark"] as const).map((t) => (
          <label key={t}>
            <input type="radio" name="theme" value={t} checked={theme === t} onChange={() => onTheme(t)} />
            {t === "system" ? "System" : t === "light" ? "Light" : "Dark"}
          </label>
        ))}
      </fieldset>
    </header>
  );
}

function ClusterSentence({ cluster }: { cluster: Cluster }) {
  const under = cluster.under_replicated_chunks;
  const pending = cluster.pending_deletions;
  const asOf = cluster.counts_as_of_unix_millis > 0 ? ` (as of ${formatTime(cluster.counts_as_of_unix_millis)})` : "";
  const replicas =
    under === 0 ? "Every chunk has its 3 replicas" : `${under} ${under === 1 ? "chunk is" : "chunks are"} under-replicated`;
  return (
    <span data-under-replicated={under} data-pending-deletions={pending}>
      {replicas}; {pending} {pending === 1 ? "file" : "files"} waiting for deletion{asOf}.
    </span>
  );
}
