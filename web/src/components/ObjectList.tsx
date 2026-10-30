import type { ObjectSummary } from "../api";
import { formatBytes } from "../format";
import { browserUnreachable } from "../keys";
import { KeyText } from "./KeyText";

interface Props {
  prefix: string;
  onPrefix: (p: string) => void;
  objects: ObjectSummary[];
  hasMore: boolean;
  loading: boolean;
  error: string | null;
  selected: string | null;
  onSelect: (key: string) => void;
  onMore: () => void;
  onRefresh: () => void;
}

export function ObjectList(p: Props) {
  return (
    <section className="objects" aria-labelledby="objects-title">
      <div className="objects-head">
        <h2 id="objects-title">Objects</h2>
        <button type="button" className="quiet" onClick={p.onRefresh}>
          Refresh
        </button>
      </div>
      <label className="field">
        <span>Filter by prefix</span>
        <input type="search" value={p.prefix} onChange={(e) => p.onPrefix(e.target.value)} placeholder="demo/" spellCheck={false} />
      </label>
      {p.error !== null ? (
        <p className="notice" data-tone="error" role="alert">
          {p.error}
        </p>
      ) : null}
      {p.objects.length === 0 && !p.loading && p.error === null ? (
        <p className="objects-empty">{p.prefix === "" ? "No objects yet. Upload a file to start." : "No keys start with this prefix."}</p>
      ) : null}
      <ul className="object-list" data-testid="object-list">
        {p.objects.map((o) => (
          <li key={o.key} data-testid="object-item" data-key={o.key}>
            {browserUnreachable(o.key) ? (
              <div className="object-row unreachable">
                <KeyText value={o.key} />
                <span className="object-note">Not reachable from a browser; use osctl.</span>
              </div>
            ) : (
              <button
                type="button"
                className="object-row"
                aria-current={p.selected === o.key ? "true" : undefined}
                onClick={() => p.onSelect(o.key)}
              >
                <KeyText value={o.key} />
                <span className="object-size">{formatBytes(o.size_bytes)}</span>
              </button>
            )}
          </li>
        ))}
      </ul>
      {p.hasMore ? (
        <button type="button" className="quiet" onClick={p.onMore} disabled={p.loading}>
          Load more
        </button>
      ) : null}
    </section>
  );
}
