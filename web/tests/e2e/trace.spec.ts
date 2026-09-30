import { expect, test, type Page } from "@playwright/test";
import { CP, HAPPY, mockRideApi, mockTraceSocket, send, TID } from "./ride-mocks";

async function simClock(page: Page) {
  await page.route("**/api/sim/clock", (r) => r.fulfill({ json: { sim_ts: "2025-10-09T17:42:00", speed: 60, paused: false, mode: "live" } }));
}

function streamRest(ws: Parameters<Parameters<typeof mockTraceSocket>[1]>[0], rest: readonly (keyof typeof CP)[]) {
  rest.forEach((s, i) => setTimeout(() => send(ws, { type: "checkpoint", ...CP[s] }), 150 * (i + 1)));
}

test("all 7 stages light up in order, with hop latencies, the total and the agent hand-off", async ({ page }) => {
  await simClock(page);
  await mockTraceSocket(page, (ws) => {
    send(ws, { type: "history", checkpoints: [CP.submitted, CP.ingested], sim_ts: "2025-10-09T17:42:00" });
    streamRest(ws, ["validated", "enriched", "aggregated", "stored", "queryable"]);
  });
  await page.goto(`/trace/${TID}`);
  await expect(page.getByText("Times Sq/Theatre District → JFK Airport")).toBeVisible();
  await expect(page.getByTestId("stage-validated")).toHaveAttribute("data-state", /next|done/);
  await expect(page.getByTestId("stage-queryable")).toHaveAttribute("data-state", "done");
  const items = page.getByTestId("stages").getByRole("listitem");
  expect(await items.evaluateAll((els) => els.map((e) => e.getAttribute("data-testid")))).toEqual(HAPPY.map((s) => `stage-${s}`));
  for (const s of HAPPY) await expect(page.getByTestId(`stage-${s}`)).toHaveAttribute("data-state", "done");
  await expect(page.getByTestId("stage-ingested")).toContainText("+41 ms");
  await expect(page.getByTestId("stage-ingested")).toContainText("partition 0, offset 482113");
  await expect(page.getByTestId("stage-enriched")).toContainText("tip (20%)");
  await expect(page.getByTestId("stage-aggregated")).toContainText("Manhattan · 17:42");
  await expect(page.getByTestId("total")).toHaveText("From your phone to queryable in 1.8 s");
  await expect(page.getByRole("link", { name: "Where does my ride rank for tips today?" }))
    .toHaveAttribute("href", `/agent?trace_id=${TID}&suggestion=ride_rank`);
});

test("an absurd tip shows the amber rejected card and the rest stays grey", async ({ page }) => {
  await simClock(page);
  await mockTraceSocket(page, (ws) => {
    send(ws, { type: "history", checkpoints: [CP.submitted, CP.ingested], sim_ts: null });
    streamRest(ws, ["rejected"]);
  });
  await page.goto(`/trace/${TID}`);
  const rejected = page.getByTestId("stage-rejected");
  await expect(rejected).toHaveAttribute("data-state", "rejected");
  await expect(rejected).toContainText("Rejected: the tip is unrealistically high.");
  await expect(page.getByTestId("stage-validated")).toHaveCount(0);
  for (const s of ["enriched", "aggregated", "stored", "queryable"]) await expect(page.getByTestId(`stage-${s}`)).toHaveAttribute("data-state", "future");
  await expect(page.getByRole("link", { name: "Book another ride" })).toHaveAttribute("href", "/ride");
  await expect(page.getByTestId("total")).toHaveCount(0);
});

test("a dropped socket reconnects and re-renders from history without duplicates", async ({ page }) => {
  await simClock(page);
  await mockTraceSocket(page, (ws, n) => {
    if (n === 0) {
      send(ws, { type: "history", checkpoints: [CP.submitted, CP.ingested], sim_ts: null });
      send(ws, { type: "checkpoint", ...CP.validated });
      setTimeout(() => ws.close(), 100);
    } else {
      send(ws, { type: "history", checkpoints: [CP.submitted, CP.ingested, CP.validated, CP.enriched, CP.aggregated], sim_ts: null });
    }
  });
  await page.goto(`/trace/${TID}`);
  await expect(page.getByTestId("stage-aggregated")).toHaveAttribute("data-state", "done");
  await expect(page.locator('[data-state="done"]')).toHaveCount(5);
  await expect(page.getByTestId("stages").getByRole("listitem")).toHaveCount(7);
});

test("while the processor is down the next card says so", async ({ page }) => {
  await simClock(page);
  await mockTraceSocket(page, (ws) => send(ws, { type: "history", checkpoints: [CP.submitted, CP.ingested], sim_ts: null }));
  await page.goto(`/trace/${TID}`);
  const next = page.getByTestId("stage-validated");
  await expect(next).toHaveAttribute("data-state", "next");
  await expect(next).toContainText("waiting for the stream processor…");
});

test("an unknown ride says so after 15 s", async ({ page }) => {
  await page.clock.install();
  await simClock(page);
  await mockTraceSocket(page, (ws) => send(ws, { type: "history", checkpoints: [], sim_ts: null }));
  await page.goto(`/trace/${TID}`);
  await expect(page.getByText("Looking for your ride…")).toBeVisible();
  await page.clock.fastForward(16_000);
  await expect(page.getByRole("link", { name: "Book a new one" })).toHaveAttribute("href", "/ride");
});

test("a malformed id never opens a socket", async ({ page }) => {
  let opened = false;
  await mockTraceSocket(page, () => { opened = true; });
  await page.goto("/trace/not-a-ride");
  await expect(page.getByRole("link", { name: "Book a new one" })).toBeVisible();
  expect(opened).toBe(false);
});

test("book on /ride and watch all 7 stages (spec 08 acceptance 1)", async ({ page }) => {
  await mockRideApi(page);
  await mockTraceSocket(page, (ws) => {
    send(ws, { type: "history", checkpoints: [CP.submitted, CP.ingested], sim_ts: null });
    streamRest(ws, ["validated", "enriched", "aggregated", "stored", "queryable"]);
  });
  await page.goto("/ride");
  await page.getByRole("button", { name: "Pickup zone" }).click();
  await page.getByRole("dialog").getByRole("region", { name: "Popular" }).getByRole("button", { name: "Times Sq/Theatre District" }).click();
  await page.getByRole("button", { name: "Drop-off zone" }).click();
  await page.getByRole("dialog").getByRole("region", { name: "Popular" }).getByRole("button", { name: "JFK Airport" }).click();
  await page.getByRole("button", { name: "Book ride" }).click();
  for (const s of HAPPY) await expect(page.getByTestId(`stage-${s}`)).toHaveAttribute("data-state", "done");
});

test("no horizontal scroll at 360 px", async ({ page }) => {
  await simClock(page);
  await mockTraceSocket(page, (ws) => send(ws, { type: "history", checkpoints: [CP.submitted, CP.ingested, CP.rejected], sim_ts: null }));
  await page.setViewportSize({ width: 360, height: 740 });
  await page.goto(`/trace/${TID}`);
  await expect(page.getByTestId("stage-rejected")).toBeVisible();
  expect(await page.evaluate(() => document.documentElement.scrollWidth - window.innerWidth)).toBeLessThanOrEqual(0);
});
