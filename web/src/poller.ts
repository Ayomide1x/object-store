// Polling, with three rules: a request starts only after the previous one
// finished (never two at once), nothing is requested while the tab is
// hidden, and failures back off: 2, 4, 8, 16, then every 30 s.

export const POLL_INTERVAL_MS = 2000;
export const MAX_BACKOFF_MS = 30_000;

export interface PollerDeps {
  /** Resolves with the data, or rejects. Gets a signal that stop() aborts. */
  load: (signal: AbortSignal) => Promise<unknown>;
  onData: (data: unknown) => void;
  onError: (error: unknown, retryInMs: number) => void;
  intervalMs?: number;
  doc?: Pick<Document, "hidden" | "addEventListener" | "removeEventListener">;
  setTimer?: (fn: () => void, ms: number) => unknown;
  clearTimer?: (id: unknown) => void;
}

/** The wait after the given number of consecutive failures (0: success). */
export function nextDelay(failures: number, intervalMs = POLL_INTERVAL_MS): number {
  if (failures === 0) {
    return intervalMs;
  }
  return Math.min(intervalMs * 2 ** (failures - 1), MAX_BACKOFF_MS);
}

/** Starts polling at once; returns stop. */
export function startPoller(deps: PollerDeps): () => void {
  const interval = deps.intervalMs ?? POLL_INTERVAL_MS;
  const doc = deps.doc ?? document;
  const setTimer = deps.setTimer ?? ((fn, ms) => setTimeout(fn, ms));
  const clearTimer = deps.clearTimer ?? ((id) => clearTimeout(id as ReturnType<typeof setTimeout>));
  const hidden = () => doc.hidden;
  const abort = new AbortController();
  let timer: unknown = null;
  let inFlight = false;
  let stopped = false;
  let failures = 0;

  const schedule = (ms: number) => {
    if (stopped || hidden()) {
      return; // visibilitychange restarts it
    }
    timer = setTimer(tick, ms);
  };

  async function tick() {
    timer = null;
    if (stopped || inFlight || hidden()) {
      return;
    }
    inFlight = true;
    try {
      const data = await deps.load(abort.signal);
      if (stopped) {
        return;
      }
      failures = 0;
      deps.onData(data);
    } catch (e) {
      if (stopped) {
        return;
      }
      failures++;
      deps.onError(e, nextDelay(failures, interval));
    } finally {
      inFlight = false;
    }
    schedule(nextDelay(failures, interval));
  }

  const onVisibility = () => {
    if (hidden()) {
      if (timer !== null) {
        clearTimer(timer);
        timer = null;
      }
    } else if (timer === null && !inFlight) {
      void tick();
    }
  };

  doc.addEventListener("visibilitychange", onVisibility);
  void tick();
  return () => {
    stopped = true;
    abort.abort();
    if (timer !== null) {
      clearTimer(timer);
    }
    doc.removeEventListener("visibilitychange", onVisibility);
  };
}
