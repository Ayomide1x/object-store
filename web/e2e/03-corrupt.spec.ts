// A corrupted replica shows as Bad, then its chunk is repaired.
import { expect, test } from "@playwright/test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { osctl, POLL_MARGIN, randomFile, sh, sql, timings } from "./stack";

const OFFSET = 56 + 1000; // past the 56-byte chunk header, inside the data

test("a corrupted replica shows as Bad, then its chunk is repaired", async ({ page }) => {
  const t = timings();
  const dir = mkdtempSync(path.join(tmpdir(), "ui-e2e-"));
  const key = `e2e/${Date.now().toString(36)}/corrupt.bin`;
  const log = path.join(dir, "flipped");
  try {
    osctl(["put", randomFile(dir, "obj", 1 << 20), key]);
    const [chunkId, victim] = sql(`SELECT c.chunk_id, min(r.node_id) FROM keys k JOIN chunks c USING (object_id)
      JOIN chunk_replicas r USING (chunk_id) WHERE k.key = '${key}' GROUP BY c.chunk_id`).split(" ");
    await page.goto(`/?key=${encodeURIComponent(key)}`);
    const row = page.getByTestId("chunk-row");
    await expect(row).toHaveAttribute("data-counted", "3");
    const cell = row.locator(`[data-testid="replica-cell"][data-node="${victim}"]`);
    await expect(cell).toHaveAttribute("data-state", "HEALTHY");

    // Flip a byte on the victim's disk; a read in listed order hits it first,
    // fails over, and reports it: the coordinator marks it BAD.
    sh(`: > ${JSON.stringify(log)}; corrupt_byte ${victim} ${chunkId} ${OFFSET} ${JSON.stringify(log)} >/dev/null`);
    osctl(["get", key, path.join(dir, "out")], { OSCTL_READ_ORDER: "listed" });

    await expect(cell).toHaveAttribute("data-state", "BAD", { timeout: POLL_MARGIN });
    await expect(cell).toHaveAttribute("aria-label", new RegExp(`on ${victim}: Bad`));

    // The report also hints the reconciler, which copies the chunk to the
    // node without a copy, usually within a second or two: too soon to wait
    // for the row's "2 of 3" in between.
    await expect(row).toHaveAttribute("data-counted", "3", { timeout: 2 * t.repairInterval + POLL_MARGIN });
    await expect(row.locator('[data-testid="replica-cell"][data-state="HEALTHY"][data-origin="repair"]')).toHaveCount(1);
    await expect(cell).toHaveAttribute("data-state", "BAD");
  } finally {
    sh(`restore_flipped ${JSON.stringify(log)} >/dev/null || true`);
    rmSync(dir, { recursive: true, force: true });
  }
});
