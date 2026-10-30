// Objects move between the browser and osctl in both directions, byte for
// byte; rm in the browser is rm for osctl.
import { expect, test, type Page } from "@playwright/test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { osctl, randomFile, sha256 } from "./stack";

let dir: string;
const run = Date.now().toString(36);

test.beforeAll(() => {
  dir = mkdtempSync(path.join(tmpdir(), "ui-e2e-"));
});

test.afterAll(() => rmSync(dir, { recursive: true, force: true }));

async function uploadInBrowser(page: Page, file: string, key: string) {
  await page.goto("/");
  await page.getByTestId("file-input").setInputFiles(file);
  await page.getByTestId("upload-key").fill(key);
  await page.getByRole("button", { name: "Upload", exact: true }).click();
  await expect(page.getByTestId("upload-status")).toHaveAttribute("data-state", "stored", { timeout: 60_000 });
}

for (const size of [0, 1, 4 << 20, 10 << 20]) {
  test(`a ${size}-byte file uploaded in the browser downloads identically with osctl`, async ({ page }) => {
    const key = `e2e/${run}/browser/ünï cödé ${size}.bin`;
    const original = randomFile(dir, `up-${size}`, size);
    await uploadInBrowser(page, original, key);

    // The upload selected the object: its grid shows every chunk at 3 of 3.
    await expect(page.getByTestId("object-detail")).toHaveAttribute("data-key", key);
    const chunks = Math.ceil(size / (4 << 20));
    await expect(page.locator('[data-testid="chunk-row"][data-counted="3"]')).toHaveCount(chunks);

    const out = path.join(dir, `down-${size}`);
    osctl(["get", key, out]);
    expect(sha256(out)).toBe(sha256(original));
  });
}

test("a file uploaded with osctl downloads identically in the browser", async ({ page }) => {
  // (Chrome replaces characters like " in the saved name; the gateway's
  // header encoding of those is covered by TestContentDispositionEncodesKey.)
  const key = `e2e/${run}/osctl/résumé (final) 100%.bin`;
  const original = randomFile(dir, "from-osctl", (9 << 20) + 7);
  osctl(["put", original, key]);

  await page.goto(`/?key=${encodeURIComponent(key)}`);
  await expect(page.locator('[data-testid="chunk-row"]')).toHaveCount(3);
  const downloading = page.waitForEvent("download");
  await page.getByTestId("download").click();
  const download = await downloading;
  expect(download.suggestedFilename()).toBe("résumé (final) 100%.bin");
  expect(await download.failure()).toBeNull();
  const saved = path.join(dir, "browser-download");
  await download.saveAs(saved);
  expect(sha256(saved)).toBe(sha256(original));
});

test("the list pages through keys and filters by prefix", async ({ page }) => {
  const prefix = `e2e/${run}/list/`;
  const tiny = randomFile(dir, "tiny", 1);
  for (let i = 0; i < 105; i++) {
    osctl(["put", tiny, `${prefix}${String(i).padStart(3, "0")}`]);
  }
  await page.goto("/");
  await page.getByLabel("Filter by prefix").fill(prefix);
  const items = page.getByTestId("object-item");
  await expect(items.first()).toHaveAttribute("data-key", `${prefix}000`);
  await expect(items).toHaveCount(100);
  await page.getByRole("button", { name: "Load more" }).click();
  await expect(items).toHaveCount(105);
  await expect(page.getByRole("button", { name: "Load more" })).toHaveCount(0);
  await expect(items.last()).toHaveAttribute("data-key", `${prefix}104`);
});

test("delete asks first, and a deleted key is gone for osctl too", async ({ page }) => {
  const key = `e2e/${run}/rm/doomed.bin`;
  osctl(["put", randomFile(dir, "doomed", 1000), key]);
  await page.goto(`/?key=${encodeURIComponent(key)}`);
  await page.getByRole("button", { name: "Delete…" }).click();

  const dialog = page.getByRole("dialog");
  await expect(dialog).toBeVisible();
  await expect(page.getByRole("button", { name: "Keep it" })).toBeFocused();
  await page.keyboard.press("Escape");
  await expect(dialog).toBeHidden();
  expect(osctl(["stat", key])).toContain(key);

  await page.getByRole("button", { name: "Delete…" }).click();
  await page.getByRole("button", { name: "Delete object" }).click();
  await expect(page.getByRole("status").filter({ hasText: "Deleted" })).toBeVisible();
  expect(() => osctl(["stat", key])).toThrow(/not found/);
});

test("the keys . and .. are refused before anything is sent", async ({ page }) => {
  await page.goto("/");
  await page.getByTestId("file-input").setInputFiles(randomFile(dir, "dots", 3));
  for (const key of [".", ".."]) {
    await page.getByTestId("upload-key").fill(key);
    await expect(page.getByTestId("upload-problem")).toContainText("can't be reached from a browser");
    await expect(page.getByRole("button", { name: "Upload", exact: true })).toBeDisabled();
  }
});
