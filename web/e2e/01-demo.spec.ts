// While scripts/demo.sh runs, the UI shows what it does: the stopped node goes
// Suspect then Dead, its chunks become under-replicated and are repaired, and
// the replica the demo corrupts shows as Bad, then its chunk is repaired.
//
// Every wait is an auto-retrying assertion on a data-* attribute, with a
// timeout derived from the Compose timings; nothing sleeps.
import { expect, test } from "@playwright/test";
import path from "node:path";
import { POLL_MARGIN, root, runDemo, timings } from "./stack";

const KEY = "demo/photo-archive.bin";
const CHUNKS = 10; // demo.sh uploads 40 MiB

test("the UI follows demo.sh: Suspect then Dead, under-replicated, repaired, Bad, repaired", async ({ page }) => {
  const t = timings();
  const demo = runDemo(5);
  try {
    await page.goto(`/?key=${encodeURIComponent(KEY)}`);

    // The upload lands: the grid fills with every chunk at 3 of 3.
    const rows = page.getByTestId("chunk-row");
    await expect(rows).toHaveCount(CHUNKS, { timeout: 90_000 });
    await expect(page.locator('[data-testid="chunk-row"][data-counted="3"]')).toHaveCount(CHUNKS);

    // A node is stopped: its card goes Suspect, then Dead.
    const suspect = page.locator('[data-testid="node-card"][data-state="SUSPECT"]');
    await expect(suspect).toHaveCount(1, { timeout: 60_000 + t.suspectAfter + POLL_MARGIN });
    const node = await suspect.getAttribute("data-node");
    expect(node).not.toBeNull();
    const card = page.locator(`[data-testid="node-card"][data-node="${node}"]`);
    await expect(card).toHaveAttribute("data-state", "DEAD", { timeout: t.deadAfter - t.suspectAfter + POLL_MARGIN });

    // The feed holds both transitions, in order (it lists newest first).
    const transitions = page.locator(`[data-testid="feed-item"][data-kind="liveness"][data-node="${node}"]`);
    await expect
      .poll(async () => (await transitions.evaluateAll((els) => els.map((e) => e.getAttribute("data-to")))).slice(0, 2))
      .toEqual(["DEAD", "SUSPECT"]);

    // How many chunks the node held: the cells in its bay that hold a copy.
    const held = await page.locator(`[data-testid="replica-cell"][data-node="${node}"][data-state="HEALTHY"]`).count();
    expect(held).toBeGreaterThan(0);

    // Past the repair grace, those chunks are under-replicated and the
    // reconciler copies them. The grid shows a chunk under-replicated only
    // until the next pass (0 to ~10 s, depending on where the pass schedule
    // falls), too short to wait on reliably; the pass itself is recorded, and
    // the feed shows it: it found exactly the chunks the node held.
    const repairPass = page.locator('[data-testid="feed-item"][data-kind="repair"]', {
      hasText: new RegExp(`found ${held} under-replicated chunks?: ${held} cop(y|ies) started, ${held} recorded`),
    });
    await expect(repairPass).toHaveCount(1, { timeout: t.repairGrace + 2 * t.repairInterval + POLL_MARGIN });

    // Repair copies land in the other bays, and every chunk is back to 3.
    await expect(page.locator('[data-testid="replica-cell"][data-state="HEALTHY"][data-origin="repair"]')).toHaveCount(held, {
      timeout: POLL_MARGIN,
    });
    await expect(page.locator(`[data-testid="replica-cell"][data-node="${node}"][data-origin="repair"]`)).toHaveCount(0);
    await expect(page.locator('[data-testid="chunk-row"][data-counted="3"]')).toHaveCount(CHUNKS, { timeout: POLL_MARGIN });
    await expect(card).toHaveAttribute("data-state", "DEAD");

    // For the README: the dead bay, the copies that replaced it, the feed.
    await page.getByRole("button", { name: "Refresh" }).click();
    await expect(page.locator(`[data-testid="object-item"][data-key="${KEY}"]`)).toHaveCount(1);
    await page.screenshot({ path: path.join(root, "web", "test-results", "ui-demo-repair.png") });
    await page.emulateMedia({ colorScheme: "dark" });
    await page.screenshot({ path: path.join(root, "web", "test-results", "ui-demo-repair-dark.png") });
    await page.emulateMedia({ colorScheme: "light" });

    // The node comes back.
    await expect(card).toHaveAttribute("data-state", "ALIVE", { timeout: 60_000 });

    // The demo corrupts a replica and reads it: it shows as Bad...
    const bad = page.locator('[data-testid="replica-cell"][data-state="BAD"]');
    await expect(bad).toHaveCount(1, { timeout: 60_000 });
    const badRow = rows.filter({ has: bad });

    // ...then its chunk is repaired onto the one node without a copy (the
    // report hints the reconciler, so the "2 of 3" between is too brief to
    // wait for).
    await expect(badRow).toHaveAttribute("data-counted", "3", { timeout: 2 * t.repairInterval + POLL_MARGIN });
    await expect(badRow.locator('[data-testid="replica-cell"][data-state="HEALTHY"][data-origin="repair"]')).not.toHaveCount(0);
    await page.screenshot({ path: path.join(root, "web", "test-results", "ui-demo-repaired.png") });

    expect(await demo.exited, demo.output()).toBe(0);
  } finally {
    demo.kill();
    await test.info().attach("demo.sh output", { body: demo.output(), contentType: "text/plain" });
  }
});
