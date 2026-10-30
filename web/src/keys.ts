// Keys are user-controlled text. On the wire a key is exactly one URL path
// segment, percent-encoded ("/" as %2F), as the gateway expects.

/** The gateway's limit (upload.MaxObjectSize): exactly 5 GiB is allowed. */
export const MAX_OBJECT_SIZE = 5 * 1024 ** 3;

/**
 * Browsers parse every URL with the WHATWG URL parser, which removes "." and
 * ".." path segments, encoded (%2E) or not. So these two keys cannot be
 * addressed from a browser at all; osctl and curl can.
 */
export function browserUnreachable(key: string): boolean {
  return key === "." || key === "..";
}

/** One path segment for key. Throws for a key a browser can't address. */
export function encodeKey(key: string): string {
  if (key === "") {
    throw new Error("a key is required");
  }
  if (browserUnreachable(key)) {
    throw new Error(`the key "${key}" can't be reached from a browser`);
  }
  return encodeURIComponent(key);
}

/** The object's URL path: /objects/{key}. */
export function objectPath(key: string): string {
  return `/objects/${encodeKey(key)}`;
}

/** Why the UI won't upload under key, or null if it will. */
export function keyProblem(key: string): string | null {
  if (key === "") {
    return "Enter a key.";
  }
  if (browserUnreachable(key)) {
    return `Keys "." and ".." can't be reached from a browser, so the UI won't upload them. Use osctl.`;
  }
  try {
    encodeKey(key);
  } catch {
    return "This key contains characters that can't be sent (an unpaired surrogate).";
  }
  return null;
}

/** Why a file of size bytes won't be sent, or null. */
export function sizeProblem(size: number): string | null {
  return size > MAX_OBJECT_SIZE ? "This file is larger than the 5 GiB limit." : null;
}
