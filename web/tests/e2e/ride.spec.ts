import { gzipSync } from "node:zlib";
import { expect, test, type Page } from "@playwright/test";
import { CREATED, mockRideApi, mockTraceSocket, send, TID } from "./ride-mocks";

async function pick(page: Page, trigger: string, search: string | null, zone: string) {
  await page.getByRole("button", { name: trigger }).click();
  const dialog = page.getByRole("dialog");
  if (search) await dialog.getByRole("searchbox", { name: "Search zones" }).fill(search);
  const scope = search ? dialog : dialog.getByRole("region", { name: "Popular" });
  await scope.getByRole("button", { name: zone, exact: true }).first().click();
  await expect(page.getByRole("dialog")).toHaveCount(0);
}

test("book a card ride: popular pickup, searched drop-off, 20 % tip → /trace/[id]", async ({ page }) => {
  const posted = await mockRideApi(page);
  await mockTraceSocket(page, (ws) => send(ws, { type: "history", checkpoints: [], sim_ts: null }));
  await page.goto("/ride");
  await pick(page, "Pickup zone", null, "Times Sq/Theatre District");
  await pick(page, "Drop-off zone", "jfk", "JFK Airport");
  await expect(page.getByTestId("estimate")).toHaveText("~3.1 mi · ~17 min · fare ~$19.50 (median of 214 real trips between these zones)");
  await page.getByRole("button", { name: "20%" }).click();
  await expect(page.getByTestId("tip-amount")).toHaveText("$3.90");
  await page.getByRole("button", { name: "More passengers" }).click();
  await expect(page.getByTestId("passengers")).toHaveText("2");
  await page.getByRole("button", { name: "Book ride" }).click();
  await expect(page).toHaveURL(new RegExp(`/trace/${TID}$`));
  expect(posted).toEqual([{ pickup_location_id: 230, dropoff_location_id: 132, payment: "card", tip_amount: 3.9, passenger_count: 2 }]);
});

test("cash shows the info line; a custom absurd tip is sent without a warning", async ({ page }) => {
  const posted = await mockRideApi(page);
  await mockTraceSocket(page, (ws) => send(ws, { type: "history", checkpoints: [], sim_ts: null }));
  await page.goto("/ride");
  await pick(page, "Pickup zone", null, "Midtown Center");
  await pick(page, "Drop-off zone", "astoria", "Astoria");
  await page.getByRole("button", { name: "Cash" }).click();
  await expect(page.getByText("Cash tips aren't recorded in NYC taxi data")).toBeVisible();
  await page.getByRole("button", { name: "Card" }).click();
  await page.getByRole("button", { name: "Custom" }).click();
  await page.getByRole("spinbutton", { name: "Custom tip in dollars" }).fill("999");
  await page.getByRole("button", { name: "Book ride" }).click();
  await expect(page).toHaveURL(/\/trace\//);
  expect(posted[0]).toMatchObject({ payment: "card", tip_amount: 999 });
});

test("a rate-limited booking shows the api's message and stays on /ride", async ({ page }) => {
  await mockRideApi(page, { status: 429, json: { detail: "You've booked 3 rides this minute. Try again in 20 s.", retry_after_s: 20 } });
  await page.goto("/ride");
  await pick(page, "Pickup zone", null, "Midtown Center");
  await pick(page, "Drop-off zone", null, "JFK Airport");
  await page.getByRole("button", { name: "Book ride" }).click();
  await expect(page.getByTestId("ride-error")).toHaveText("You've booked 3 rides this minute. Try again in 20 s.");
  await expect(page).toHaveURL(/\/ride$/);
});

test("Book ride waits for both zones", async ({ page }) => {
  await mockRideApi(page);
  await page.goto("/ride");
  await expect(page.getByRole("button", { name: "Book ride" })).toBeDisabled();
});

test("reopening /ride offers to continue watching the last ride", async ({ page }) => {
  await mockRideApi(page);
  await page.addInitScript((id) => localStorage.setItem("nyc-taxi-live:last-trace", id), TID);
  await page.goto("/ride");
  await expect(page.getByRole("link", { name: "Continue watching your ride" })).toHaveAttribute("href", CREATED.trace_url);
});

test("no horizontal scroll at 360 px, closed and with the picker open", async ({ page }) => {
  await mockRideApi(page);
  await page.setViewportSize({ width: 360, height: 740 });
  await page.goto("/ride");
  const overflow = () => page.evaluate(() => document.documentElement.scrollWidth - window.innerWidth);
  expect(await overflow()).toBeLessThanOrEqual(0);
  await page.getByRole("button", { name: "Pickup zone" }).click();
  expect(await overflow()).toBeLessThanOrEqual(0);
});

test("first-load JS is at most 150 kB gzipped (spec 08 acceptance 4)", async ({ page }) => {
  await mockRideApi(page);
  const bodies: Promise<Buffer>[] = [];
  page.on("response", (r) => {
    if (r.request().resourceType() === "script") bodies.push(r.body());
  });
  await page.goto("/ride", { waitUntil: "networkidle" });
  const total = (await Promise.all(bodies)).reduce((n, b) => n + gzipSync(b).length, 0);
  console.log(`/ride first-load JS: ${(total / 1024).toFixed(1)} kB gzipped`);
  expect(total).toBeLessThanOrEqual(150 * 1024);
});
