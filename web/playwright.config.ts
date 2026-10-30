import { defineConfig, devices } from "@playwright/test";

// End-to-end tests against the real Compose stack (scripts/stage11-ui-check.sh
// starts a fresh one). One worker, files in name order: 01-demo needs the
// fresh cluster demo.sh insists on, so it runs first.
export default defineConfig({
  testDir: "e2e",
  fullyParallel: false,
  workers: 1,
  retries: 0,
  forbidOnly: true,
  timeout: 8 * 60_000,
  reporter: [["list"]],
  outputDir: "test-results",
  use: {
    baseURL: process.env.UI_URL ?? "http://127.0.0.1:9081",
    trace: "retain-on-failure",
    ...devices["Desktop Chrome"],
    viewport: { width: 1440, height: 900 },
    // PW_CHANNEL=chrome runs the installed Google Chrome instead of
    // Playwright's Chromium.
    channel: process.env.PW_CHANNEL,
  },
});
