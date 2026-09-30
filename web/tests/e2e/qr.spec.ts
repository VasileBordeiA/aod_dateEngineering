import { expect, test } from "@playwright/test";

// playwright.config.ts starts the server with STAND_WIFI_SSID=Test;Net and STAND_PUBLIC_URL=http://192.168.8.10
test("two big QR codes: join the Wi-Fi, then open /ride", async ({ page }) => {
  await page.goto("/qr");
  await expect(page.getByTestId("qr-wifi").locator("svg")).toBeVisible();
  await expect(page.getByTestId("qr-wifi")).toContainText("Test;Net");
  await expect(page.getByTestId("qr-ride").locator("svg")).toBeVisible();
  await expect(page.getByTestId("qr-ride")).toContainText("http://192.168.8.10/ride");
  const box = await page.getByTestId("qr-ride").locator("svg").boundingBox();
  expect(box!.width).toBeGreaterThanOrEqual(300); // scannable from a step away
});
