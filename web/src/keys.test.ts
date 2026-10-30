import { describe, expect, it } from "vitest";
import { encodeKey, keyProblem, MAX_OBJECT_SIZE, objectPath, sizeProblem } from "./keys";

describe("encodeKey", () => {
  // Mutation u1: encodeURI instead of encodeURIComponent.
  it("encodes reserved characters so a key stays one path segment", () => {
    expect(encodeKey("demo/photo.bin")).toBe("demo%2Fphoto.bin");
    expect(encodeKey("a?b#c")).toBe("a%3Fb%23c");
    expect(encodeKey("100%")).toBe("100%25");
    expect(encodeKey("with space")).toBe("with%20space");
    expect(encodeKey("a+b&c=d;e")).toBe("a%2Bb%26c%3Dd%3Be");
    expect(encodeKey("résumé")).toBe("r%C3%A9sum%C3%A9");
    expect(objectPath("logs/2026/10/30.txt")).toBe("/objects/logs%2F2026%2F10%2F30.txt");
  });

  it("round-trips through the URL parser unchanged", () => {
    for (const key of ["a/b", "a/../b", "./x", "x/.", "..a", "%2E%2E", "日本/語", "a\\b", "#?&"]) {
      const url = new URL(objectPath(key), "http://127.0.0.1:9081");
      expect(url.pathname.split("/")).toHaveLength(3);
      expect(decodeURIComponent(url.pathname.slice("/objects/".length))).toBe(key);
    }
  });

  // Mutation u2: the refusal of dot segments is removed.
  it("refuses dot segments, which the URL parser would remove", () => {
    expect(() => encodeKey(".")).toThrow(/can't be reached from a browser/);
    expect(() => encodeKey("..")).toThrow(/can't be reached from a browser/);
    expect(keyProblem("..")).toMatch(/Use osctl/);
    expect(keyProblem(".")).toMatch(/Use osctl/);
    expect(keyProblem("...")).toBeNull();
  });

  it("refuses an empty key and unsendable text", () => {
    expect(keyProblem("")).toBe("Enter a key.");
    expect(keyProblem("\uD800")).toMatch(/can't be sent/);
  });
});

describe("sizeProblem", () => {
  // Mutation u8: >= instead of >.
  it("allows exactly 5 GiB and refuses one byte more", () => {
    expect(sizeProblem(0)).toBeNull();
    expect(sizeProblem(MAX_OBJECT_SIZE)).toBeNull();
    expect(sizeProblem(MAX_OBJECT_SIZE + 1)).toBe("This file is larger than the 5 GiB limit.");
  });
});
