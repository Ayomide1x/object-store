import { describe, expect, it } from "vitest";
import { formatBytes, formatDuration } from "./format";

describe("formatBytes", () => {
  it("uses binary units", () => {
    expect([0, 1, 1023, 1024, 1536, 4 << 20, 40 << 20, 5 * 1024 ** 3].map(formatBytes)).toEqual([
      "0 bytes", "1 byte", "1023 bytes", "1 KiB", "1.5 KiB", "4 MiB", "40 MiB", "5 GiB",
    ]);
  });
});

describe("formatDuration", () => {
  it("rounds down to seconds, minutes or hours", () => {
    expect([0, 999, 42_000, 119_999, 180_000, 7_200_000].map(formatDuration)).toEqual(["0 s", "0 s", "42 s", "119 s", "3 min", "2 h"]);
  });
});
