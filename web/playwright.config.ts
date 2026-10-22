import { defineConfig, devices } from "@playwright/test";

const PHONES = /(ride|trace|agent-phone)\.spec\.ts/;

export default defineConfig({
  testDir: "tests/e2e",
  use: { baseURL: "http://127.0.0.1:3100" },
  projects: [
    { name: "desktop", testMatch: /(agent|live|qr)\.spec\.ts/, use: { ...devices["Desktop Chrome"], viewport: { width: 1920, height: 1080 } } },
    // spec 08 acceptance 1: iPhone 13 and Pixel 7 viewports. Chromium for both, so no WebKit download is needed offline.
    { name: "iphone-13", testMatch: PHONES, use: { ...devices["iPhone 13"], browserName: "chromium" } },
    { name: "pixel-7", testMatch: PHONES, use: { ...devices["Pixel 7"] } },
  ],
  webServer: {
    command: "pnpm build && pnpm start -p 3100",
    url: "http://127.0.0.1:3100/agent",
    reuseExistingServer: !process.env.CI,
    timeout: 180_000,
    env: { STAND_PUBLIC_URL: "http://192.168.8.10" },
  },
});
