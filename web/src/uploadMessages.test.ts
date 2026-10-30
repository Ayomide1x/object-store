import { describe, expect, it } from "vitest";
import { describeUpload, INTERRUPTED, OUTCOME_UNKNOWN, parseRetryAfter } from "./uploadMessages";

describe("describeUpload", () => {
  it("says a connection dropped before the whole file was sent stored nothing, and offers Retry", () => {
    const n = describeUpload({ kind: "dropped", sentAll: false });
    expect(n.text).toBe(
      "Upload interrupted before the whole file was sent; nothing was stored. The gateway may have been busy: try again.",
    );
    expect(n.text).toBe(INTERRUPTED);
    expect(n.retry).toBe(true);
    expect(n.retryAfterSeconds).toBe(0);
  });

  // Mutation u7: a drop after the whole body was sent is reported as "nothing stored".
  it("says a drop after the whole file was sent is outcome-unknown, never 'nothing stored'", () => {
    const n = describeUpload({ kind: "dropped", sentAll: true });
    expect(n.text).toBe(OUTCOME_UNKNOWN);
    expect(n.text).not.toMatch(/nothing was stored/);
    expect(n.tone).toBe("unknown");
    expect(n.retry).toBe(true);
  });

  // Mutation u6: Retry-After is ignored.
  it("uses Retry-After for a busy gateway", () => {
    const n = describeUpload({ kind: "http", status: 503, code: "BUSY", message: "gateway is serving 8 uploads", retryAfterSeconds: 1 });
    expect(n.text).toBe("Gateway busy: it is already handling its limit of uploads. Nothing was stored. Retry in 1 s.");
    expect(n.retry).toBe(true);
    expect(n.retryAfterSeconds).toBe(1);
  });

  it("offers Retry at once for a 503 without Retry-After", () => {
    const n = describeUpload({ kind: "http", status: 503, code: "UNAVAILABLE", message: "fewer than 2 replicas acknowledged", retryAfterSeconds: null });
    expect(n.text).toBe("The cluster couldn't store the file: fewer than 2 replicas acknowledged. Nothing was stored. Try again.");
    expect(n.retryAfterSeconds).toBe(0);
  });

  it("explains 411 and 413 and offers no Retry for them", () => {
    const n411 = describeUpload({ kind: "http", status: 411, code: "LENGTH_REQUIRED", message: "x", retryAfterSeconds: null });
    expect(n411.text).toMatch(/sent no file length/);
    expect(n411.retry).toBe(false);
    const n413 = describeUpload({ kind: "http", status: 413, code: "TOO_LARGE", message: "x", retryAfterSeconds: null });
    expect(n413.text).toBe("This file is larger than the 5 GiB limit. Nothing was stored.");
    expect(n413.retry).toBe(false);
  });

  it("reports success", () => {
    expect(describeUpload({ kind: "stored", objectId: "o", sizeBytes: 1 })).toMatchObject({ tone: "ok", retry: false });
  });
});

describe("parseRetryAfter", () => {
  it("reads whole seconds and ignores anything else", () => {
    expect(parseRetryAfter("1")).toBe(1);
    expect(parseRetryAfter(" 5 ")).toBe(5);
    expect(parseRetryAfter(null)).toBeNull();
    expect(parseRetryAfter("Wed, 21 Oct 2026 07:28:00 GMT")).toBeNull();
    expect(parseRetryAfter("-1")).toBeNull();
  });
});
