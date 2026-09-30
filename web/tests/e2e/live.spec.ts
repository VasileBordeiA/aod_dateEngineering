import { expect, test } from "@playwright/test";

const TID = "3f2c9a1e-8b7d-4c21-9e55-0a1b2c3d4e5f";
const at = () => new Date().toISOString();
const student = (stage: string, seq: number, detail: Record<string, unknown> = {}) =>
  ({ type: "checkpoint", trace_id: TID, trip_id: `s-${TID}`, source: "student", stage, seq, service: "processor", at: at(), detail, ambient: false });

test("student dots move box to box, ambient dots are small, and the side column shows today's numbers", async ({ page }) => {
  await page.routeWebSocket(/\/api\/trace\/live/, (ws) => {
    ws.send(JSON.stringify({ type: "snapshot", traces: [], sim_ts: "2025-10-09T17:42:00" }));
    ws.send(JSON.stringify({ type: "stats", rides_today: 57, rejected_today: 6, median_ms: { ingested: 40, validated: 62 } }));
    ws.send(JSON.stringify(student("submitted", 1)));
    ws.send(JSON.stringify({ type: "checkpoint", trace_id: "9d1e0000-0000-4000-8000-000000000000", trip_id: "r3-000000042", source: "replay",
      stage: "ingested", seq: 2, service: "replayer", at: at(), detail: {}, ambient: true }));
    setTimeout(() => ws.send(JSON.stringify(student("enriched", 4, { pu_borough: "Manhattan" }))), 300);
  });
  await page.goto("/trace/live");
  await expect(page.getByTestId("box-enriched").getByTestId("dot")).toHaveText("3f2c · Manhattan");
  await expect(page.getByTestId("box-submitted").getByTestId("dot")).toHaveCount(0);
  await expect(page.getByTestId("box-ingested").locator('[data-ambient="true"]')).toHaveCount(1);
  await expect(page.getByTestId("rides-today")).toHaveText("57");
  await expect(page.getByTestId("rejected-today")).toHaveText("6");
  await expect(page.getByTestId("box-validated")).toContainText("62 ms");
  await expect(page.getByTestId("ride-qr").locator("svg")).toBeVisible();
  await expect(page.getByTestId("ride-qr")).toContainText("http://192.168.8.10/ride");
});
