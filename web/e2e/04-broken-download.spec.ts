// A download that breaks partway is a failed download in the browser, never
// a complete-looking file.
import { expect, test } from "@playwright/test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { osctl, randomFile, sh, sql } from "./stack";

const OFFSET = 56 + 1000;

test("a download whose later chunk has no good replica fails in the browser", async ({ page }) => {
  const dir = mkdtempSync(path.join(tmpdir(), "ui-e2e-"));
  const key = `e2e/${Date.now().toString(36)}/broken.bin`;
  const log = path.join(dir, "flipped");
  try {
    osctl(["put", randomFile(dir, "obj", (8 << 20) + 1), key]);
    // Every replica of chunk 1 (the second of three): chunk 0 streams first.
    const replicas = sql(`SELECT r.node_id, c.chunk_id FROM keys k JOIN chunks c USING (object_id)
      JOIN chunk_replicas r USING (chunk_id) WHERE k.key = '${key}' AND c.chunk_index = 1`).split("\n");
    expect(replicas).toHaveLength(3);
    sh(`: > ${JSON.stringify(log)}; ` + replicas.map((r) => `corrupt_byte ${r} ${OFFSET} ${JSON.stringify(log)} >/dev/null;`).join(" "));

    await page.goto(`/?key=${encodeURIComponent(key)}`);
    const downloading = page.waitForEvent("download");
    await page.getByTestId("download").click();
    const download = await downloading;
    // Mutation: the gateway ends the response normally instead of aborting.
    expect(await download.failure()).not.toBeNull();
  } finally {
    sh(`restore_flipped ${JSON.stringify(log)} >/dev/null || true`);
    rmSync(dir, { recursive: true, force: true });
  }
});
