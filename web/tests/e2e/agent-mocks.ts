import type { Page } from "@playwright/test";

export const SUGGESTIONS = [
  { id: "best_tippers", question: "Which borough has the best tippers?" },
  { id: "busiest_zones_last_hour", question: "What are the 10 busiest pickup zones in the last hour?" },
];

/** Mocks every /api call /agent makes; /api/agent/ask streams `askBody`. */
export async function mockApi(page: Page, askBody: string) {
  await page.route("**/api/suggestions", (r) => r.fulfill({ json: SUGGESTIONS }));
  await page.route("**/api/llm/status", (r) =>
    r.fulfill({ json: { providers: [{ name: "local", model: "qwen2.5-coder:14b", healthy: true, last_ok_at: null, p50_ms: 5000 }], order: ["local"], active: "local", mode: "tools" } }));
  await page.route("**/api/sim/clock", (r) => r.fulfill({ json: { sim_ts: "2025-10-09T17:42:00", speed: 60, paused: false, mode: "live" } }));
  await page.route("**/api/agent/ask", (r) => r.fulfill({ status: 200, contentType: "text/event-stream", body: askBody }));
}
