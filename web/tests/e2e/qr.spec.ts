import { expect, test } from "@playwright/test";

// playwright.config.ts starts the server with STAND_PUBLIC_URL=http://192.168.8.10
test("one big QR code that opens /ride, and no Wi-Fi code (ADR-025)", async ({ page }) => {
  await page.goto("/qr");
  await expect(page.getByTestId("qr-ride").locator("svg")).toBeVisible();
  await expect(page.getByTestId("qr-ride")).toContainText("http://192.168.8.10/ride");
  await expect(page.getByTestId("qr-wifi")).toHaveCount(0);
  await expect(page.getByText("Wi-Fi")).toHaveCount(0);
  const box = await page.getByTestId("qr-ride").locator("svg").boundingBox();
  expect(box!.width).toBeGreaterThanOrEqual(300); // scannable from a step away
});
