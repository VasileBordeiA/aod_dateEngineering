import { defineConfig, devices } from "@playwright/test";

export default defineConfig({
  testDir: "tests/e2e",
  use: { ...devices["Desktop Chrome"], baseURL: "http://127.0.0.1:3100", viewport: { width: 1920, height: 1080 } },
  webServer: {
    command: "pnpm build && pnpm start -p 3100",
    url: "http://127.0.0.1:3100/agent",
    reuseExistingServer: !process.env.CI,
    timeout: 180_000,
  },
});
