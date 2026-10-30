import react from "@vitejs/plugin-react";
import { defineConfig } from "vitest/config";

// The dev server proxies the API to a running gateway (GATEWAY, default
// gateway1). changeOrigin rewrites Host to the gateway's own address, which
// its GATEWAY_ALLOWED_HOSTS allows; the browser sees one origin (Vite's), so
// its requests are same-origin and pass the gateway's cross-origin check.
const gateway = process.env.GATEWAY ?? "http://127.0.0.1:9081";
const api = { target: gateway, changeOrigin: true };

export default defineConfig({
  plugins: [react()],
  build: {
    // Embedded by the gateway (gateway/internal/webui).
    outDir: "../gateway/internal/webui/build/dist",
    emptyOutDir: true,
    assetsInlineLimit: 0, // every asset a file, so the CSP needs no data: URLs
  },
  server: {
    host: "127.0.0.1",
    port: 5173,
    strictPort: true,
    proxy: { "/objects": api, "/cluster": api, "/healthz": api },
  },
  test: {
    environment: "jsdom",
    include: ["src/**/*.test.{ts,tsx}"],
    setupFiles: ["src/test-setup.ts"],
  },
});
