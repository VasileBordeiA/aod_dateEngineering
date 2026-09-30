import type { Page, WebSocketRoute } from "@playwright/test";

export const TID = "3f2c9a1e-8b7d-4c21-9e55-0a1b2c3d4e5f";
// api order: sorted by borough, then zone
export const ZONES = [
  { location_id: 255, zone: "Williamsburg (North Side)", borough: "Brooklyn" },
  { location_id: 79, zone: "East Village", borough: "Manhattan" },
  { location_id: 161, zone: "Midtown Center", borough: "Manhattan" },
  { location_id: 230, zone: "Times Sq/Theatre District", borough: "Manhattan" },
  { location_id: 7, zone: "Astoria", borough: "Queens" },
  { location_id: 132, zone: "JFK Airport", borough: "Queens" },
  { location_id: 138, zone: "LaGuardia Airport", borough: "Queens" },
];
export const ESTIMATE = { trip_distance: 3.1, duration_min: 16.5, fare_amount: 19.5, method: "median of 214 real trips between these zones" };
export const CREATED = {
  trace_id: TID, trip_id: `s-${TID}`, trace_url: `/trace/${TID}`, pickup_ts: "2025-10-09T17:42:00",
  estimate: { ...ESTIMATE, total_amount: 29.25 }, notes: [],
};

/** Mocks every /api call /ride makes. Returns the POST bodies it received. */
export async function mockRideApi(page: Page, post: { status: number; json: unknown } = { status: 201, json: CREATED }) {
  const posted: unknown[] = [];
  await page.route("**/api/zones", (r) => r.fulfill({ json: ZONES }));
  await page.route("**/api/rides/estimate**", (r) => r.fulfill({ json: ESTIMATE }));
  await page.route("**/api/rides", (r) => {
    posted.push(r.request().postDataJSON());
    return r.fulfill({ status: post.status, json: post.json });
  });
  await page.route("**/api/sim/clock", (r) => r.fulfill({ json: { sim_ts: "2025-10-09T17:42:00", speed: 60, paused: false, mode: "live" } }));
  return posted;
}

export async function mockTraceSocket(page: Page, onConnect: (ws: WebSocketRoute, n: number) => void) {
  let n = 0;
  await page.routeWebSocket(/\/api\/trace\//, (ws) => onConnect(ws, n++));
}

export const send = (ws: WebSocketRoute, msg: unknown) => ws.send(JSON.stringify(msg));
