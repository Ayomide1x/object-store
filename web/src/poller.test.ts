import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { nextDelay, startPoller } from "./poller";

class FakeDoc extends EventTarget {
  hidden = false;
  setHidden(h: boolean) {
    this.hidden = h;
    this.dispatchEvent(new Event("visibilitychange"));
  }
}

beforeEach(() => vi.useFakeTimers());
afterEach(() => vi.useRealTimers());

/** A load whose calls resolve or reject only when the test says. */
function manualLoad() {
  const pending: { resolve: (v: unknown) => void; reject: (e: unknown) => void }[] = [];
  const load = vi.fn(() => new Promise((resolve, reject) => pending.push({ resolve, reject })));
  return { load, pending };
}

describe("startPoller", () => {
  it("never starts a request while one is in flight", async () => {
    const { load, pending } = manualLoad();
    const doc = new FakeDoc();
    const stop = startPoller({ load, onData: () => {}, onError: () => {}, doc });
    expect(load).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(10_000);
    expect(load).toHaveBeenCalledTimes(1);
    pending[0]?.resolve({});
    await vi.advanceTimersByTimeAsync(1999);
    expect(load).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(1);
    expect(load).toHaveBeenCalledTimes(2);
    stop();
  });

  it("stops while the tab is hidden and polls at once when it is shown", async () => {
    const load = vi.fn(async () => ({}));
    const doc = new FakeDoc();
    const stop = startPoller({ load, onData: () => {}, onError: () => {}, doc });
    await vi.advanceTimersByTimeAsync(0);
    doc.setHidden(true);
    await vi.advanceTimersByTimeAsync(60_000);
    expect(load).toHaveBeenCalledTimes(1);
    doc.setHidden(false);
    await vi.advanceTimersByTimeAsync(0);
    expect(load).toHaveBeenCalledTimes(2);
    stop();
  });

  it("backs off on failures and resets after a success", async () => {
    let fail = true;
    const load = vi.fn(async () => {
      if (fail) {
        throw new Error("down");
      }
      return {};
    });
    const waits: number[] = [];
    const doc = new FakeDoc();
    const stop = startPoller({ load, onData: () => {}, onError: (_e, ms) => waits.push(ms), doc });
    await vi.advanceTimersByTimeAsync(0);
    for (const ms of [2000, 4000, 8000, 16_000, 30_000]) {
      await vi.advanceTimersByTimeAsync(ms);
    }
    expect(waits).toEqual([2000, 4000, 8000, 16_000, 30_000, 30_000]);
    fail = false;
    await vi.advanceTimersByTimeAsync(30_000);
    const calls = load.mock.calls.length;
    await vi.advanceTimersByTimeAsync(2000);
    expect(load.mock.calls.length).toBe(calls + 1);
    stop();
  });

  it("delivers nothing after stop", async () => {
    const { load, pending } = manualLoad();
    const onData = vi.fn();
    const stop = startPoller({ load, onData, onError: () => {}, doc: new FakeDoc() });
    stop();
    pending[0]?.resolve({});
    await vi.advanceTimersByTimeAsync(10_000);
    expect(onData).not.toHaveBeenCalled();
    expect(load).toHaveBeenCalledTimes(1);
  });
});

describe("nextDelay", () => {
  it("is the interval after a success and doubles to a 30 s cap", () => {
    expect([0, 1, 2, 3, 4, 5, 9].map((f) => nextDelay(f))).toEqual([2000, 2000, 4000, 8000, 16_000, 30_000, 30_000]);
  });
});
