import { readFileSync } from "node:fs";
import { join } from "node:path";
import { expect, test, type Page } from "@playwright/test";

// Playwright loads this spec as CommonJS (package.json has no "type": "module"), so __dirname, not import.meta.url.
const FIXTURE = readFileSync(join(__dirname, "../fixtures/agent-best-tippers.sse"), "utf8");
const SUGGESTIONS = [
  { id: "best_tippers", question: "Which borough has the best tippers?" },
  { id: "busiest_zones_last_hour", question: "What are the 10 busiest pickup zones in the last hour?" },
];

async function mockApi(page: Page, askBody: string) {
  await page.route("**/api/suggestions", (r) => r.fulfill({ json: SUGGESTIONS }));
  await page.route("**/api/llm/status", (r) =>
    r.fulfill({ json: { providers: [{ name: "local", model: "qwen2.5-coder:14b", healthy: true, last_ok_at: null, p50_ms: 5000 }], order: ["local"], active: "local", mode: "tools" } }));
  await page.route("**/api/sim/clock", (r) => r.fulfill({ json: { sim_ts: "2025-10-09T17:42:00", speed: 60, paused: false, mode: "live" } }));
  await page.route("**/api/agent/ask", (r) => r.fulfill({ status: 200, contentType: "text/event-stream", body: askBody }));
}

test("a suggested question renders steps, both SQL attempts, table, chart and answer", async ({ page }) => {
  await mockApi(page, FIXTURE);
  await page.goto("/agent");
  await expect(page.getByText("Thu 09 Oct 17:42")).toBeVisible();
  await page.getByRole("button", { name: "Which borough has the best tippers?" }).click();

  const answer = page.getByTestId("answer");
  await expect(answer).toContainText("Manhattan riders tip best: 23.1%");
  await expect(answer).toContainText("answered by qwen2.5-coder:14b (local) · 6.2 s");

  const sql = page.getByTestId("sql-panel");
  await expect(sql.locator("del")).toContainText("avg(tip)");
  await expect(sql).toContainText("Unknown expression identifier");
  await expect(sql).toContainText("attempt 2/3");
  await expect(sql).toContainText("+ safety LIMIT 200");

  await expect(page.getByRole("table")).toContainText("Staten Island");
  await expect(page.locator(".recharts-surface").first()).toBeVisible();
  await expect(page.getByTestId("steps")).toContainText("describe_table(trips)");
  await expect(page.getByTestId("steps")).toContainText("Fixing its own query…");
});

test("idle reset clears the screen after 90 s and shows the attract state", async ({ page }) => {
  await page.clock.install();
  await mockApi(page, FIXTURE);
  await page.goto("/agent");
  await page.getByRole("button", { name: "Which borough has the best tippers?" }).click();
  await expect(page.getByTestId("answer")).toContainText("Manhattan");
  await page.clock.fastForward(91_000);
  await expect(page.getByTestId("attract")).toBeVisible();
  await expect(page.getByText("Manhattan riders tip best")).toHaveCount(0);
});

test("rate limiting shows a friendly countdown", async ({ page }) => {
  await mockApi(page, 'event: error\ndata: {"code":"RATE_LIMITED","message":"Lots of questions right now! Try again in 12 s.","retry_after_s":12}\n\nevent: done\ndata: {"request_id":"r","provider":"","model":"","mode":"","cached":false,"latency_ms":1,"tool_calls":0,"invalid_tool_calls":0,"repairs":0}\n\n');
  await page.goto("/agent");
  await page.getByRole("textbox").fill("How many trips?");
  await page.getByRole("button", { name: "Ask", exact: true }).click();
  await expect(page.getByText(/Try again in \d+ s/)).toBeVisible();
});

test("a cut stream keeps what arrived and offers a retry", async ({ page }) => {
  await mockApi(page, FIXTURE.split("event: answer")[0] ?? "");
  await page.goto("/agent");
  await page.getByRole("button", { name: "Which borough has the best tippers?" }).click();
  await expect(page.getByRole("table")).toContainText("Manhattan");
  await expect(page.getByRole("button", { name: "Try again" })).toBeVisible();
});
