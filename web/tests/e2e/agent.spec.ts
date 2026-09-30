import { readFileSync } from "node:fs";
import { join } from "node:path";
import { expect, test, type Page } from "@playwright/test";

// Playwright loads this spec as CommonJS (package.json has no "type": "module"), so __dirname, not import.meta.url.
const FIXTURE = readFileSync(join(__dirname, "../fixtures/agent-best-tippers.sse"), "utf8");
const ONESHOT_FIXTURE = readFileSync(join(__dirname, "../fixtures/agent-oneshot-repair.sse"), "utf8");
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
  await expect(sql).toContainText("attempt 2");
  await expect(sql).not.toContainText("/3");
  await expect(sql).toContainText("+ safety LIMIT 200");

  await expect(page.getByRole("table")).toContainText("Staten Island");
  await expect(page.locator(".recharts-surface").first()).toBeVisible();
  await expect(page.getByTestId("steps")).toContainText("describe_table(trips)");
  await expect(page.getByTestId("steps")).toContainText("Fixing its own query…");
});

test("oneshot: a failed attempt shows its error line, and the steps list the run_query steps", async ({ page }) => {
  await mockApi(page, ONESHOT_FIXTURE);
  await page.goto("/agent");
  await page.getByRole("button", { name: "Which borough has the best tippers?" }).click();
  await expect(page.getByTestId("answer")).toContainText("answered by taxi-qwen2.5-coder-14b (local) · 7.4 s");

  const sql = page.getByTestId("sql-panel");
  await expect(sql.locator("del")).toHaveCount(1);
  await expect(sql.locator("del")).toContainText("avg(tip)");
  await expect(sql).toContainText("✗ Code: 47. DB::Exception: Unknown expression identifier `tip`");
  await expect(sql).toContainText("attempt 2");

  const steps = page.getByTestId("steps");
  await expect(steps).toContainText("Using the schema cheat-sheet");
  await expect(steps.getByText("run_query", { exact: true })).toHaveCount(2);
  await expect(steps.locator('[aria-label="failed"]')).toHaveCount(1);
  await expect(steps).toContainText("Unknown expression identifier");
  await expect(steps).toContainText("5 rows in 84 ms");
  await expect(steps).toContainText("render_chart(bar)");
});

test("oneshot: when no query works, both attempts are struck through with their errors", async ({ page }) => {
  const frame = (event: string, data: unknown) => `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
  const attempt = (n: number, sql: string, error: string) =>
    frame("tool_call", { id: `oneshot_${n}`, name: "run_query", arguments: { sql, purpose: "x" } }) +
    frame("sql", { attempt: n, sql, purpose: "x" }) +
    frame("tool_result", { id: `oneshot_${n}`, name: "run_query", ok: false, summary: error });
  await mockApi(page,
    frame("status", { text: "Using the schema cheat-sheet" }) +
    attempt(1, "SELECT * FROM system.users", "The table system.users is not allowed.") +
    frame("status", { text: "Fixing its own query…" }) +
    attempt(2, "SELECT * FROM system.grants", "The table system.grants is not allowed.") +
    frame("error", { code: "NO_WORKING_QUERY", message: "I couldn't write a working query for that. Try rephrasing, or pick a suggested question!" }) +
    frame("done", { request_id: "n", provider: "local", model: "m", mode: "oneshot", cached: false, latency_ms: 10, tool_calls: 0, invalid_tool_calls: 0, repairs: 2 }));
  await page.goto("/agent");
  await page.getByRole("button", { name: "Which borough has the best tippers?" }).click();
  const sql = page.getByTestId("sql-panel");
  await expect(sql.locator("del")).toHaveCount(2);
  await expect(sql).toContainText("✗ The table system.users is not allowed.");
  await expect(sql).toContainText("✗ The table system.grants is not allowed.");
  await expect(page.getByTestId("steps").locator('[aria-label="failed"]')).toHaveCount(2);
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

test("a typed question is cleared from the input box, and stays cleared after the idle reset (invariant 8)", async ({ page }) => {
  await page.clock.install();
  await mockApi(page, FIXTURE);
  await page.goto("/agent");
  const box = page.getByRole("textbox");
  await box.fill("Where does my cousin Ana usually take a taxi?");
  await page.getByRole("button", { name: "Ask", exact: true }).click();
  await expect(page.getByTestId("answer")).toContainText("Manhattan riders tip best");
  await expect(box).toHaveValue("");

  await page.clock.fastForward(91_000);
  await expect(page.getByTestId("attract")).toBeVisible();
  await expect(page.getByText("cousin Ana")).toHaveCount(0);
  await page.mouse.click(10, 10); // tap outside the question: wakes the kiosk to type
  await expect(page.getByTestId("attract")).toHaveCount(0);
  await expect(page.getByRole("textbox")).toHaveValue("");
});

test("events from a replaced stream never reach the new question", async ({ page }) => {
  await mockApi(page, FIXTURE);
  const frame = (event: string, data: unknown) => `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
  const B_BODY =
    frame("sql", { attempt: 1, sql: "SELECT 'B-ONLY' AS b", purpose: "B" }) +
    frame("answer", { text: "Answer for question B." }) +
    frame("done", { request_id: "b", provider: "local", model: "m", mode: "tools", cached: false, latency_ms: 10, tool_calls: 1, invalid_tool_calls: 0, repairs: 0 });
  // Question A streams from a stream the test controls and that ignores abort, so frames can still
  // arrive after B has started: exactly what a parser's queued frames look like to the kiosk.
  await page.addInitScript((bBody: string) => {
    const w = window as unknown as { __pushA?: (s: string) => void; fetch: typeof fetch };
    const realFetch = window.fetch.bind(window);
    w.fetch = async (input: RequestInfo | URL, init?: RequestInit) => {
      if (String(input).endsWith("/api/agent/ask")) {
        const { question } = JSON.parse(String(init?.body)) as { question: string };
        const headers = { "Content-Type": "text/event-stream" };
        if (question.includes("best tippers")) {
          const enc = new TextEncoder();
          const stream = new ReadableStream<Uint8Array>({ start: (c) => { w.__pushA = (s) => c.enqueue(enc.encode(s)); } });
          return new Response(stream, { headers });
        }
        return new Response(bBody, { headers });
      }
      return realFetch(input, init);
    };
  }, B_BODY);
  await page.goto("/agent");
  const sql = page.getByTestId("sql-panel");

  await page.getByRole("button", { name: "Which borough has the best tippers?" }).click();
  await page.waitForFunction(() => typeof (window as unknown as { __pushA?: unknown }).__pushA === "function");
  await page.evaluate((f) => (window as unknown as { __pushA: (s: string) => void }).__pushA(f),
    frame("sql", { attempt: 1, sql: "SELECT 'A-EARLY' AS a", purpose: "A" }));
  await expect(sql).toContainText("A-EARLY");

  await page.getByRole("button", { name: "What are the 10 busiest pickup zones in the last hour?" }).click();
  await expect(page.getByTestId("answer")).toContainText("Answer for question B.");
  await expect(sql).toContainText("B-ONLY");
  await expect(sql).not.toContainText("A-EARLY");

  // A keeps talking after B started: its frames must be ignored.
  await page.evaluate((f) => (window as unknown as { __pushA: (s: string) => void }).__pushA(f),
    frame("sql", { attempt: 2, sql: "SELECT 'A-LATE' AS a", purpose: "A" }) +
    frame("answer", { text: "Answer for question A." }) +
    frame("done", { request_id: "a", provider: "local", model: "m", mode: "tools", cached: false, latency_ms: 10, tool_calls: 1, invalid_tool_calls: 0, repairs: 0 }));
  // Let the page drain its microtasks and paint twice before checking that nothing of A landed.
  await page.evaluate(() => new Promise<void>((r) => requestAnimationFrame(() => requestAnimationFrame(() => r()))));
  await expect(sql).not.toContainText("A-LATE");
  await expect(page.getByTestId("answer")).not.toContainText("question A");
  await expect(page.getByTestId("answer")).toContainText("Answer for question B.");
  await expect(sql).not.toContainText("attempt 2");
});

const RIDE_TID = "3f2c9a1e-8b7d-4c21-9e55-0a1b2c3d4e5f";
const RIDE_SUBMITTED = {
  trace_id: RIDE_TID, trip_id: `s-${RIDE_TID}`, source: "student", stage: "submitted", seq: 1, service: "api",
  at: "2026-10-16T10:31:07.000Z",
  detail: { pickup_zone: "Midtown Center", dropoff_zone: "JFK Airport", payment: "card", tip_amount: 5, fare_amount: 70.5 },
};

test("the ride hand-off shows the ride chip and asks with the ride context", async ({ page }) => {
  await mockApi(page, FIXTURE);
  await page.route(`**/api/trace/${RIDE_TID}`, (r) => r.fulfill({ json: { trace_id: RIDE_TID, checkpoints: [RIDE_SUBMITTED] } }));
  let body: unknown;
  await page.route("**/api/agent/ask", async (r) => {
    body = r.request().postDataJSON();
    await r.fulfill({ status: 200, contentType: "text/event-stream", body: FIXTURE });
  });
  await page.goto(`/agent?trace_id=${RIDE_TID}&suggestion=ride_rank`);
  await expect(page.getByTestId("ride-chip")).toHaveText("Your ride: Midtown Center → JFK Airport, $5 tip");
  await page.getByTestId("ride-prompt").getByRole("button", { name: "Where does my ride rank for tips today?" }).click();
  await expect(page.getByTestId("answer")).toBeVisible();
  expect(body).toEqual({ question: "Where does my ride rank for tips today?", origin: "ride", trace_id: RIDE_TID });
});

test("a malformed ride id is ignored: no prompt, normal kiosk", async ({ page }) => {
  await mockApi(page, FIXTURE);
  await page.goto("/agent?trace_id=not-a-uuid&suggestion=ride_rank");
  await expect(page.getByRole("button", { name: "Which borough has the best tippers?" })).toBeVisible();
  await expect(page.getByTestId("ride-prompt")).toHaveCount(0);
});
