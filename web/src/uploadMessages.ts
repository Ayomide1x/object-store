// What an upload's outcome means to the person who started it.

/** How an upload ended. */
export type UploadOutcome =
  | { kind: "stored"; objectId: string; sizeBytes: number }
  | { kind: "http"; status: number; code: string; message: string; retryAfterSeconds: number | null }
  // No response. sentAll: the browser had sent the whole body first.
  | { kind: "dropped"; sentAll: boolean };

export interface Notice {
  tone: "ok" | "error" | "unknown";
  text: string;
  /** Offer a Retry button. */
  retry: boolean;
  /** Seconds before Retry is enabled (from Retry-After), or 0. */
  retryAfterSeconds: number;
}

export const INTERRUPTED =
  "Upload interrupted before the whole file was sent; nothing was stored. The gateway may have been busy: try again.";

export const OUTCOME_UNKNOWN =
  "Connection lost after the whole file was sent, so whether it was stored is unknown. " +
  "Check the object's committed time below, or upload it again.";

/** Parses a Retry-After header in seconds (the gateway never sends a date). */
export function parseRetryAfter(header: string | null): number | null {
  if (header === null || !/^\d+$/.test(header.trim())) {
    return null;
  }
  return Number(header.trim());
}

export function describeUpload(outcome: UploadOutcome): Notice {
  switch (outcome.kind) {
    case "stored":
      return { tone: "ok", text: "Stored.", retry: false, retryAfterSeconds: 0 };
    case "dropped":
      // Before the whole body was sent the gateway cannot have committed: it
      // completes an upload only after reading every Content-Length byte.
      return outcome.sentAll
        ? { tone: "unknown", text: OUTCOME_UNKNOWN, retry: true, retryAfterSeconds: 0 }
        : { tone: "error", text: INTERRUPTED, retry: true, retryAfterSeconds: 0 };
    case "http":
      return describeStatus(outcome);
  }
}

function describeStatus(o: Extract<UploadOutcome, { kind: "http" }>): Notice {
  const wait = o.retryAfterSeconds ?? 0;
  const later = (text: string): Notice => ({ tone: "error", text, retry: true, retryAfterSeconds: wait });
  const final = (text: string): Notice => ({ tone: "error", text, retry: false, retryAfterSeconds: 0 });
  const retryIn = wait > 0 ? ` Retry in ${wait} s.` : " Try again.";
  switch (o.status) {
    case 411:
      return final("The browser sent no file length, so the gateway refused the upload. Nothing was stored.");
    case 413:
      return final("This file is larger than the 5 GiB limit. Nothing was stored.");
    case 503:
      if (o.code === "BUSY") {
        return later(`Gateway busy: it is already handling its limit of uploads. Nothing was stored.${retryIn}`);
      }
      return later(`The cluster couldn't store the file: ${o.message}. Nothing was stored.${retryIn}`);
    case 400:
      return final(`The gateway refused the upload: ${o.message}.`);
    case 403:
      return final(`The gateway refused the upload: ${o.message}.`);
    default:
      if (o.status >= 500) {
        return later(`The upload failed (${o.status}): ${o.message}. Nothing was stored.${retryIn}`);
      }
      return final(`The upload failed (${o.status}): ${o.message}.`);
  }
}
