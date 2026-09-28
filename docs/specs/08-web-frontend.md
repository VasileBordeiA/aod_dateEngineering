# Spec 08: Web frontend (Next.js + TypeScript)

## Purpose
All visitor-facing screens except Grafana: the agent kiosk, the mobile ride form, the trace viewers, the clickable architecture, and the QR display. The UI must be **legible from a distance, instant, and honest**: it shows the SQL, the stages, and the rejections.

## Responsibilities
- Pages and components listed below, a typed API client (SSE + WebSocket), and the fixed visual language.
- No business logic: all rules live in the api. The frontend renders events.

## Interfaces (API dependencies)
| Endpoint | Used by | Spec |
|---|---|---|
| `POST /api/agent/ask` (SSE), `GET /api/suggestions`, `GET /api/llm/status` | `/agent` | 06 |
| `GET /api/zones`, `GET /api/rides/estimate`, `POST /api/rides` | `/ride` | 07 |
| `WS /api/trace/{id}` | `/trace/[id]` | 07 |
| `WS /api/trace/live` | `/trace/live` | 07 |
| `GET /api/stats` | `/architecture`, `/agent` header | this spec (below), implemented in api |
| `GET /api/sim/clock` | header sim clock on every page | 07 |

`GET /api/stats` is computed by the api from ClickHouse and cached for 2 s:
```json
{ "sim_ts": "2025-10-09T17:42:00", "trips_per_s": 96.4, "rejected_per_s": 3.1, "total_trips": 1834221,
  "freshness_ms": 820, "student_rides_today": 57, "agent_questions_today": 212 }
```

## Config
- `NEXT_PUBLIC_STAND_URL` (= `STAND_PUBLIC_URL`) for the QR codes.
- `NEXT_PUBLIC_GRAFANA_PATH=/grafana/d/nyc-taxi-live?kiosk&refresh=5s`.
- `STAND_WIFI_SSID` and `STAND_WIFI_PASSWORD` for the `/qr` Wi-Fi code (read on the server side only, in the `/qr` server component).
- Dev only: `API_ORIGIN=http://localhost:8000`, used by Next `rewrites` for `/api/*`. WebSocket in dev connects directly to `ws://localhost:8000`. In the stand setup, Caddy serves everything from one origin.

## Behaviour

### Pages
| Route | Device | Content |
|---|---|---|
| `/` | any | A hub with three big tiles: Ask the data, Trace your ride, Live dashboard |
| `/agent` | stand laptop (1440p+), also works on phones | The agent kiosk. See the layout below |
| `/ride` | **phone** (from the QR code) | The booking form. See below |
| `/trace/[id]` | phone | The student's ride moving through the stages |
| `/trace/live` | tablet or second screen | The wall of recent student rides crossing the pipeline |
| `/architecture` | stand laptop | Clickable diagram with live counters |
| `/qr` | tablet | Two large QR codes: "1. Join Wi-Fi" (`WIFI:T:WPA;S:…;P:…;;`) and "2. Open `<stand>/ride`" |

### `/agent` layout (kiosk)
```
┌──────────────────────────────────────────────────────────────────────────────┐
│ NYC Taxi Live · Ask the data      sim clock Thu 09 Oct 17:42   ● local model │
├───────────────────────────────┬──────────────────────────────────────────────┤
│ [ Ask anything about NYC taxis… ]│  SQL  (+ safety LIMIT 200)     attempt 1/3 │
│ (Which borough tips best?)       │  SELECT pu_borough, round(avg(tip_pct),1) │
│ (Distance at rush hour?)         │  FROM taxi.trips WHERE is_card …          │
│ (Busiest zones last hour?)       │──────────────────────────────────────────│
│ (Airport vs other trips?)        │  ▇▇▇▇▇▇▇▇▇ Manhattan 23.1%                │
│ (What did the pipeline reject?)  │  ▇▇▇▇▇▇▇ Brooklyn 19.4%       [chart]     │
│                                  │  …                                        │
│ Steps                            │──────────────────────────────────────────│
│ ✓ list_tables       6 tables     │  Manhattan riders tip best: 23.1% of the │
│ ✓ describe_table    trips        │  fare on average. Note: only card tips   │
│ ✓ run_query         5 rows 84 ms │  are recorded in NYC taxi data.          │
│ ✓ render_chart                   │  answered by qwen2.5-coder:14b · 6.2 s    │
└──────────────────────────────────┴──────────────────────────────────────────┘
```
- **The SQL panel is always visible.** It has syntax highlighting, the attempt counter, and a "+ safety LIMIT 200" badge. Failed attempts are shown struck-through with the error line, because the self-repair is part of the show.
- The step timeline animates as SSE events arrive. `status` lines are shown in grey.
- The chart is rendered from the `chart` + `rows` events: `bar` (horizontal if category labels are longer than 12 characters), `line`, `number` (big KPI), or `table`. Values are formatted by `unit`.
- Answer text is at least 28 px on the kiosk.
- **Idle reset.** After 90 s without interaction, clear the screen and show an "attract" state that cycles the suggested questions.
- The model badge comes from `/api/llm/status`: green is local, blue is internal, amber is "cached answers only".
- Errors are friendly, never raw stack traces. `RATE_LIMITED` shows a countdown.
- `?trace_id=…&suggestion=ride_rank` preselects the "rank my ride" question and shows a ride chip: "Your ride: Midtown → JFK, $5 tip".

### `/ride` (mobile-first, 360 px minimum width)
1. **Pickup zone** and **Dropoff zone**: a searchable combobox grouped by borough. A "Popular" row offers JFK Airport, LaGuardia Airport, Times Sq/Theatre District, Midtown Center, East Village, Williamsburg (North Side).
2. **Estimate card**, from `GET /api/rides/estimate`: "~3.1 mi · ~17 min · fare ~$19.50 (median of 214 real trips)".
3. **Payment**: a Card/Cash toggle. Picking cash shows the info line "Cash tips aren't recorded in NYC taxi data".
4. **Tip**: chips for $0, 15%, 20%, 25%, and Custom. Custom allows up to $999, and there's no warning about absurd values. The processor will reject them, and that's the lesson.
5. **Passengers**: 1–6 stepper, default 1.
6. **Book ride** (big button) posts, then navigates to `/trace/[id]`. The last `trace_id` is saved in `localStorage` (inside try/catch) so reopening shows "Continue watching your ride".

There's no name field and no free text anywhere.

### `/trace/[id]` (phone)
- The header shows the ride summary (zones, fare, tip) and the sim clock.
- A vertical pipeline of 7 stage cards (`submitted` → `queryable`). A small "packet" dot travels down between cards.
- When a checkpoint arrives, its card lights up (green), shows the stage copy from spec 07 with the `detail` values filled in, and shows **"+{ms} ms"** since the previous stage.
- `rejected` shows an amber card with the reason, the explanation, and a "Book another ride" button. The remaining cards stay greyed.
- At `queryable`: a total latency banner ("From your phone to queryable in 1.8 s") and the CTA "Where does my ride rank for tips today?" linking to `/agent?trace_id=…&suggestion=ride_rank`.
- The WebSocket reconnects with backoff (0.5, 1, 2, 5 s), rendering from `history` on reconnect.
- `prefers-reduced-motion` means no travelling dot, only a state change.

### `/trace/live` (big screen)
- The stage boxes run horizontally, in the same shape and colours as the architecture image.
- Each student ride is a labelled dot (the first 4 characters of its trace_id plus the pickup borough) that jumps box to box as its checkpoints arrive. It fades out 20 s after its terminal stage.
- Ambient replay-sampled dots are small and translucent, so the wall is never empty.
- Side column: rides today, rejected today, median latency per hop, and a QR code for `/ride`.

### `/architecture`
- An SVG replica of the architecture image (see [DIAGRAMS §1](../DIAGRAMS.md#1-system-architecture)). Each box is clickable, and a side drawer opens with:
  - what it does (2 sentences),
  - which tech it uses and why (1 sentence),
  - a live number from `/api/stats` (for example, Redpanda shows "96 trips/s entering"),
  - a link to go deeper: Redpanda Console (laptop only), the Grafana panel, or `/trace/live`.
- The drawer copy comes from [DEMO_SCRIPT](../DEMO_SCRIPT.md) §Talking points, so the two stay in sync.

### Tech choices
- Next.js App Router, `output: "standalone"` (for the Docker image), TypeScript `strict`, pnpm, Tailwind.
- Charts: Recharts. SQL highlighting: `prism-react-renderer` or `shiki` (pick the smaller one). Animation: `motion` (framer-motion). QR: `qrcode.react`.
- **SSE over POST:** `fetch` + a `ReadableStream` parser (about 40 lines, in `lib/sse.ts`), because `EventSource` can't POST. Unit-test the parser.
- A typed API client in `lib/api.ts`. TypeScript types for checkpoints and trips are generated from `contracts/events/*.schema.json` with `json-schema-to-typescript` (`pnpm gen:types`).
- Use the `setup-nextjs-project` skill to scaffold, the `frontend-design` skill for the visual pass, and the `dataviz` skill before building charts.

### Visual language
- **Dark theme** by default (stands are bright, so test on the actual screen).
- Colours from the architecture image:
  - Core pipeline: purple `#3C3489` (fill), `#534AB7` (stroke), `#EEEDFE` (text).
  - Visitor-facing: green `#0B4D3F`, `#1D9E75`, `#E1F5EE`.
  - Source: grey `#46463F`.
  - Rejection: amber, not red.
- **One fixed colour per borough**, shared with Grafana (spec 05). Define it in `lib/palette.ts`.
- Kiosk base font 20 px. Phone base font 16 px. Tap targets ≥ 44 px.
- WCAG AA contrast. The UI must be usable with no instructions.

## Failure modes
| Failure | UI behaviour |
|---|---|
| api unreachable | A full-width banner: "Reconnecting to the pipeline…" with auto-retry. Suggestions stay visible |
| SSE stream cut mid-answer | Show what arrived, plus a "Try again" button |
| WS dropped | Reconnect with backoff and re-render from history |
| Slow LLM | Keep the step timeline alive with `status` events. Show elapsed seconds after 5 s |

## Observability
The frontend has no analytics or tracking. The `/agent` footer shows the provider, model, and latency, which comes from `done`.

## Acceptance criteria
1. Playwright (iPhone 13 and Pixel 7 viewports) with a mocked API: book a ride and see all 7 stages render in order. The rejected path shows the amber card and stops.
2. Playwright on `/agent` with a recorded SSE fixture: the SQL panel, the table, the chart, and the answer all render. Failed attempts show struck-through. The idle reset triggers at 90 s (fake timers).
3. No horizontal scroll at 360 px on `/ride` and `/trace/[id]`.
4. The `/ride` first-load JS is ≤ 150 kB gzipped. Lighthouse mobile performance is ≥ 90 on the stand network.
5. Unit tests for the SSE parser: chunked frames, multi-line data, and a stream ending without a trailing newline.
6. Types are generated from `contracts/` and compile. Changing a contract field breaks `pnpm typecheck`.
7. Everything works through Caddy on one origin, with the Wi-Fi internet uplink unplugged.

## Out of scope
i18n (English only), accounts, a PWA or offline mode on phones, and analytics.
