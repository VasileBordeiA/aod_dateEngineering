# Runbook: operating the stand

**Roles:**
- **Operator:** owns the laptop, watches Stand Health, and runs recovery.
- **Presenter(s):** talk to students and follow [DEMO_SCRIPT](DEMO_SCRIPT.md).

With only one person, check Stand Health between visitors.

Everything the operator types is a `make` target. **Don't improvise commands at the stand.** If something isn't in this runbook, use the fallback video and fix it after.

---

## 1. One week before
- [ ] Venue check, if possible: power sockets, table size, screen/TV input (HDMI?), and whether phones on the venue Wi-Fi can reach each other (they usually can't, which is why we bring a router).
- [ ] Confirm with IT whether the internal AI gateway may be used from the venue, and whether an uplink is available.
- [ ] **Print** (A3 poster + A5 table cards):
  - the architecture poster (DIAGRAMS §1),
  - "How it works in 4 steps",
  - the QR card: ① join Wi-Fi `NYC-Taxi-Live`, ② open `http://192.168.8.10/ride`.
- [ ] 10-hour soak passed on **the event laptop** (spec 10). Report committed.
- [ ] Fallback video recorded (3 min: dashboard → agent question → ride trace).

## 2. The evening before (at home, with internet)
```bash
git pull
make images                          # build + pull every image (no internet needed tomorrow)
ollama pull qwen2.5-coder:14b
make ollama-model BASE=qwen2.5-coder:14b   # creates taxi-qwen2.5-coder-14b (num_ctx 16384), once per laptop
make data                            # no-op if already prepared
make up
make backfill UNTIL=2025-10-14T07:00:00   # ~7 sim days of history (measured M1: 2 sim days = 283k rows in 23 s incl. processor drain → expect ~1.5 min)
make preflight                       # must be all green
make eval ONLY=suggested             # 5/5
make down                            # volumes keep everything
```
- [ ] The replayer state now points at **Tue 14 Oct 2025, 07:00**, so the stand opens at morning rush hour.
- [ ] Charge the tablet and a phone for testing. Pack the list below.

**Packing list:**
- laptop + charger
- travel router + its power + an Ethernet cable + a USB-C Ethernet adapter
- HDMI cable + adapter
- tablet + stand
- extension cord / power strip
- laptop stand or fan
- printed poster and cards, tape
- a USB SSD with the repo, images (`docker save`), and the fallback video
- a phone for tethering (the uplink fallback)

## 3. Morning setup (T−60 min)
1. **Physical:**
   - Router powered, with the laptop plugged in by Ethernet.
   - Laptop on power. External screen = Grafana, laptop screen = `/agent`, tablet = `/trace/live` or `/qr`.
2. **Uplink (optional):** router repeater to venue Wi-Fi, or phone USB tether. Skip it if you're local-only.
3. Start everything:
   ```bash
   ollama serve        # if not running as a service (see spec 09 for env vars)
   make up             # also starts caffeinate
   make preflight
   ```
4. Open the kiosk windows:
   ```bash
   make kiosk          # Chrome kiosk: /agent on laptop, /grafana/d/nyc-taxi-live?kiosk on external screen
   ```
5. **Phone test:** join `NYC-Taxi-Live`, scan the QR code, book a ride, and watch all 7 stages. Ask "Where does my ride rank for tips today?"
6. Press each of the 5 suggestion buttons once. This warms the model and checks for 5 answers.

### What `make preflight` checks (all must be ✅)
| Check | Pass condition |
|---|---|
| Containers | every service `healthy`, 0 restarts |
| Disk | ≥ 20 GB free |
| Topics | 6 topics exist. Processor lag < 1 000 |
| Sim clock | two reads 3 s apart: Δsim ≈ 3 × REPLAY_SPEED (±20%). Not paused |
| Freshness | `now64() − max(stored_at)` < 3 s |
| ClickHouse | `taxi.zones` = 265 rows. `agent_ro` cannot INSERT (negative test) |
| LLM | `/api/llm/status` shows an active provider healthy. `ollama ps` shows the model loaded |
| Agent | the 5 suggested questions pass their eval checks |
| Ride E2E | `POST /api/rides` reaches `queryable` in < 5 s |
| Edge | `http://$STAND_PUBLIC_URL/`, `/agent`, `/ride`, `/grafana/api/health` all 200 via the LAN IP |
| Kiosk safety | `caffeinate` running. Do Not Disturb on (manual reminder) |

## 4. During the day
- Every hour, glance at **Stand Health** (`http://localhost:3002/d/stand-health?kiosk`, user `admin`, password `GRAFANA_ADMIN_PASSWORD` from `.env`): freshness is green, agent success is ≥ 90%, and memory is flat. It runs in its own Grafana on the laptop only, so visitors can never open it (ADR-017).
- If the replayer reaches the end of the month, it loops by itself: the sim clock jumps about 4 days forward, to the same weekday 5 weeks later. The dashboard shows a date jump, and that's expected. You don't need to do anything.
- Avoid pausing the replayer during visits. The dashboard freezes.
- A crowd arrives after a lecture: point them to the QR card. The rate limits protect the laptop. When the queue is long, the agent says "You're next in line…", which is expected.

## 5. Recovery playbook
| Symptom | Likely cause | Action |
|---|---|---|
| Dashboard frozen, sim clock not moving | Replayer paused or crashed | `make status`, then `make resume`. If it crashed: `docker compose restart replayer` |
| Dashboard "No data" | ClickHouse restarting | Wait 60 s. If it's still down: `docker compose restart clickhouse`. Kafka buffers everything |
| Freshness climbing (> 10 s) | Processor stuck or slow | `make logs S=processor`, then `docker compose restart processor`. It resumes from offsets, with no loss |
| Agent: "AI is taking a break" | Ollama down or overloaded | `ollama ps`. Restart `ollama serve`. Meanwhile suggestions work from cache. Optionally `make llm ORDER=internal,local` |
| Agent answers slowly (> 20 s) | Model unloaded (cold) or thermal throttling | Press a suggestion to warm it. Check the fan/stand. Consider switching to the internal provider |
| Agent answers wrong | It happens | **Use it**: "Let's read the SQL. What did it misunderstand?" Then press a suggestion |
| Phones can't open the ride page | Wrong Wi-Fi, or firewall | Check that the phone is on `NYC-Taxi-Live`. `curl http://192.168.8.10/ride` from the laptop. Check the macOS firewall allows Docker |
| Router dead | Hardware | Fallback: phone hotspot (≤ 10 clients). Students use `/agent` on the laptop instead |
| Trace stuck at `ingested` | Processor down | See "Freshness climbing" above. The trace resumes automatically |
| Docker Desktop frozen | It happens on macOS | Quit Docker Desktop fully and reopen, then `make up`. Volumes persist |
| Laptop dead | n/a | Play the fallback video on the tablet and walk through the printed poster |

**Other controls:**
- Switch LLM provider live: `make llm ORDER=internal,local` (calls `POST /api/admin/llm/order`).
- Change replay speed: `make speed S=120` (faster = more action on the dashboard), `make speed S=60` to go back.
- Reset student rides, if they skew stats or for a clean afternoon: `make reset-rides`. This runs `DELETE FROM taxi.trips WHERE source='student'` as admin.

## 6. End of day
```bash
make export-log      # taxi.agent_log → exports/agent_log-<date>.csv (questions only, no personal data)
make status > exports/status-<date>.txt
make down            # stops caffeinate too
```
- Note: what broke, which questions students asked most, and how many rides and questions there were. Put it in `docs/event-retro-<date>.md`.

## 7. Chaos drill results (fill in during M5)
| Drill | Date run | Real recovery time | Notes |
|---|---|---|---|
| Processor crash | | | |
| ClickHouse outage 60 s | | | |
| Broker restart | | | |
| No internet | | | |
| Ollama dies | | | |
| API restart mid-trace | | | |
| Laptop lid / sleep | | | |
