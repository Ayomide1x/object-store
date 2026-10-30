import type { Stat } from "../api";
import { formatBytes, formatTime } from "../format";
import { objectPath } from "../keys";
import { KeyText } from "./KeyText";

interface Props {
  stat: Stat;
  onDelete: () => void;
}

export function ObjectDetail({ stat, onDelete }: Props) {
  const chunks = stat.chunks.length;
  return (
    <div className="detail" data-testid="object-detail" data-key={stat.key}>
      <h2 className="detail-key">
        <KeyText value={stat.key} />
      </h2>
      <p className="detail-facts">
        {formatBytes(stat.size_bytes)} in {chunks} {chunks === 1 ? "chunk" : "chunks"}, committed{" "}
        <time dateTime={new Date(stat.committed_at_unix_millis).toISOString()}>{formatTime(stat.committed_at_unix_millis)}</time>
      </p>
      <p className="detail-id">
        Object <span className="mono">{stat.object_id}</span>
      </p>
      <div className="detail-actions">
        {/* A plain link: the browser streams the download to disk. */}
        <a className="button" href={objectPath(stat.key)} download data-testid="download">
          Download
        </a>
        <button type="button" className="danger-quiet" onClick={onDelete}>
          Delete…
        </button>
      </div>
      <p className="detail-hint">
        If a download breaks partway, your browser marks it failed. The gateway never finishes a download it couldn't verify.
      </p>
    </div>
  );
}
