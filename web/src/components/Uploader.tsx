import { useEffect, useRef, useState } from "react";
import { formatBytes } from "../format";
import { keyProblem, sizeProblem } from "../keys";
import { startUpload, type Progress } from "../upload";
import { describeUpload, type Notice } from "../uploadMessages";

interface Props {
  prefix: string;
  /** Called after an upload that stored, or may have stored, its key. */
  onSettled: (key: string) => void;
}

type Phase =
  | { name: "idle" }
  | { name: "chosen"; file: File }
  | { name: "sending"; file: File; progress: Progress }
  | { name: "done"; file: File; notice: Notice; at: number };

export function Uploader({ prefix, onSettled }: Props) {
  const input = useRef<HTMLInputElement>(null);
  const [phase, setPhase] = useState<Phase>({ name: "idle" });
  const [key, setKey] = useState("");
  const [dragging, setDragging] = useState(false);

  function choose(file: File | undefined) {
    if (file === undefined) {
      return;
    }
    setKey(prefix + file.name);
    setPhase({ name: "chosen", file });
  }

  async function send(file: File) {
    setPhase({ name: "sending", file, progress: { sent: 0, total: file.size } });
    const run = startUpload(key, file, (progress) => setPhase({ name: "sending", file, progress }));
    const outcome = await run.done;
    setPhase({ name: "done", file, notice: describeUpload(outcome), at: Date.now() });
    if (outcome.kind === "stored" || (outcome.kind === "dropped" && outcome.sentAll)) {
      onSettled(key);
    }
  }

  const file = phase.name === "idle" ? null : phase.file;
  const problem = file === null ? null : (keyProblem(key) ?? sizeProblem(file.size));

  return (
    <section
      className="uploader"
      data-dragging={dragging}
      onDragOver={(e) => {
        e.preventDefault();
        setDragging(true);
      }}
      onDragLeave={() => setDragging(false)}
      onDrop={(e) => {
        e.preventDefault();
        setDragging(false);
        if (phase.name !== "sending") {
          choose(e.dataTransfer.files[0]);
        }
      }}
      aria-label="Upload"
    >
      <input
        ref={input}
        type="file"
        className="visually-hidden"
        tabIndex={-1}
        data-testid="file-input"
        onChange={(e) => {
          choose(e.target.files?.[0]);
          e.target.value = "";
        }}
      />
      {phase.name === "idle" ? (
        <div className="uploader-idle">
          <button type="button" className="primary" onClick={() => input.current?.click()}>
            Upload file
          </button>
          <span className="uploader-hint">or drop one here</span>
        </div>
      ) : (
        <form
          className="uploader-form"
          onSubmit={(e) => {
            e.preventDefault();
            if (problem === null && file !== null && phase.name !== "sending") {
              void send(file);
            }
          }}
        >
          <p className="uploader-file">
            {file?.name} <span className="muted">({formatBytes(file?.size ?? 0)})</span>
          </p>
          <label className="field">
            <span>Key</span>
            <input
              value={key}
              onChange={(e) => setKey(e.target.value)}
              disabled={phase.name === "sending"}
              spellCheck={false}
              data-testid="upload-key"
              aria-describedby={problem !== null ? "upload-problem" : undefined}
            />
          </label>
          {problem !== null ? (
            <p id="upload-problem" className="notice" data-tone="error" data-testid="upload-problem">
              {problem}
            </p>
          ) : null}
          {phase.name === "sending" ? <SendProgress progress={phase.progress} /> : null}
          {phase.name === "done" ? <Outcome notice={phase.notice} at={phase.at} onRetry={() => void send(phase.file)} /> : null}
          <div className="uploader-actions">
            {phase.name === "chosen" ? (
              <button type="submit" className="primary" disabled={problem !== null}>
                Upload
              </button>
            ) : null}
            {phase.name !== "sending" ? (
              <button type="button" className="quiet" onClick={() => setPhase({ name: "idle" })}>
                {phase.name === "done" ? "Done" : "Cancel"}
              </button>
            ) : null}
          </div>
        </form>
      )}
    </section>
  );
}

function SendProgress({ progress }: { progress: Progress }) {
  const all = progress.sent >= progress.total;
  return (
    <div className="progress" data-testid="upload-status" data-state={all ? "storing" : "sending"}>
      <progress max={Math.max(progress.total, 1)} value={progress.sent} aria-label="Upload progress" />
      <p aria-live="polite">
        {all ? "Sent. Storing…" : `Sent ${formatBytes(progress.sent)} of ${formatBytes(progress.total)}`}
      </p>
    </div>
  );
}

function Outcome({ notice, at, onRetry }: { notice: Notice; at: number; onRetry: () => void }) {
  const [now, setNow] = useState(Date.now);
  const readyAt = at + notice.retryAfterSeconds * 1000;
  useEffect(() => {
    if (now >= readyAt) {
      return;
    }
    const t = setTimeout(() => setNow(Date.now()), Math.min(250, readyAt - now));
    return () => clearTimeout(t);
  }, [now, readyAt]);
  const waiting = now < readyAt;
  const state = notice.tone === "ok" ? "stored" : notice.tone === "unknown" ? "unknown" : "failed";
  return (
    <div className="outcome">
      <p className="notice" data-tone={notice.tone} data-testid="upload-status" data-state={state} role="status">
        {notice.text}
      </p>
      {notice.retry ? (
        <button type="button" onClick={onRetry} disabled={waiting} data-testid="upload-retry">
          {waiting ? `Retry in ${Math.ceil((readyAt - now) / 1000)} s` : "Retry"}
        </button>
      ) : null}
    </div>
  );
}
