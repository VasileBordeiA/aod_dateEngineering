# Tutorial: run and demo NYC Taxi Live

A hands-on walkthrough, from a cold laptop to a finished 3-minute demo. For the full stand procedure see [RUNBOOK](RUNBOOK.md). For what to say, see [DEMO_SCRIPT](DEMO_SCRIPT.md).

Everything below was checked against this machine on 2026-09-30.

---

## 0. What's already on this machine

| Item | State |
|---|---|
| Tools | Docker 29.6.2, uv 0.11.13, pnpm 10.33.2, Node 24.13.1, Python 3.12.13, Ollama 0.30.8, make 3.81 |
| `.env` | exists (passwords filled) |
| TLC data | `data/raw/yellow_tripdata_2025-10.parquet` (75 MB), `data/replay/trips_2025-10.parquet` (107 MB), `data/zones.csv` (265 zones) |
| Docker images | all built or pulled: api, web, processor, replayer, 2× grafana, ClickHouse 26.3.34, Redpanda v26.2.3, Console v3.12.0, Caddy 2.11.4 |
| Ollama models | **`taxi-qwen2.5-coder-14b`** (active one), plus `taxi-qwen3-14b`, `taxi-llama3.1-8b` and the base models |
| Milestones | M0–M4 merged. M5 was dropped, so pre-event testing is manual (ADR-021) |
| Disk | 518 GB free (need ≥ 20 GB) |

**The stack was already running when I checked**: 10 containers healthy, the sim clock moving at 60×, about 5.5 M trips and 647 k rejects in ClickHouse, and the agent's local provider healthy. Ollama had **no model loaded**, so the first agent question will be slow. Warm it up (step 3).

Nothing is missing. You can skip Part A unless you're setting up a fresh laptop.

---

## Part A: fresh setup (only on a new machine)

```bash
cp .env.example .env && make secrets    # creates .env and fills empty passwords
brew install ollama
ollama pull qwen2.5-coder:14b
make ollama-model BASE=qwen2.5-coder:14b  # derives taxi-qwen2.5-coder-14b (16k context)
make data                                 # downloads and prepares the TLC month
make images                               # builds and pulls all images (needs internet)
```

Check that `.env` has `LLM_LOCAL_MODEL=taxi-qwen2.5-coder-14b`. `LLM_INTERNAL_*` is optional, because the demo must work offline.

---

## Part B: start it

### 1. Start Ollama (native, for the Metal GPU)
```bash
ollama serve            # skip if it already runs as a service
curl -s localhost:11434/v1/models | head -c 200
```

### 2. Start the stack
```bash
make up                 # docker compose up -d --build --wait, plus caffeinate
make status             # every service healthy, processor lag < 1000
```
If Docker Desktop isn't running, start it first. Volumes persist across `make down`, so the replay position and history survive.

### 3. Warm the model
A cold 14B model takes a while to load. Do this before anyone is watching:
```bash
curl -s localhost:11434/api/generate -d '{"model":"taxi-qwen2.5-coder-14b","prompt":"hi","stream":false}' | head -c 100
ollama ps               # the model should now be listed
```

### 4. (Optional) Pre-load history
Makes the dashboard show a full morning instead of starting empty:
```bash
make backfill UNTIL=2025-10-14T07:00:00    # about 1.5 min, leaves the clock at Tue 14 Oct 07:00
```
Note that the running stack is currently at **22 Nov 2025**, because the replayer has looped once, and that's expected. To go back to rush hour, run `make reset` first. **It deletes all volumes**, so only do it if you mean to.

### 5. Pre-flight checks (30 seconds)
```bash
curl -s http://127.0.0.1:8000/api/healthz          # all true
curl -s http://127.0.0.1:8000/api/sim/clock        # run twice, 3 s apart: sim_ts advances about 180 s
curl -s http://127.0.0.1:8000/api/llm/status       # active provider healthy
curl -s http://localhost/grafana/api/health        # database: ok
make test-stack                                    # automated checks on the running stack
```

---

## Part C: the demo, screen by screen

| Screen | URL |
|---|---|
| Live dashboard (big screen) | http://localhost/grafana/d/nyc-taxi-live?kiosk |
| AI agent (laptop) | http://localhost/agent |
| Ride form (phones) | `http://<laptop-ip>/ride` (or show the QR at `/qr`) |
| Live trace wall (tablet) | http://localhost/trace/live |
| Redpanda Console (behind the scenes) | http://localhost:8080 |
| Stand Health (operator only) | http://localhost:3002/d/stand-health?kiosk, user `admin`, password = `GRAFANA_ADMIN_PASSWORD` in `.env` |

`make kiosk` and `make export-log` are mentioned in the docs but **do not exist in the Makefile yet**. Open the windows by hand.

### The 3-minute flow
1. **Hook (0:00).** Point at the dashboard: "That's real NYC taxi traffic from Oct 2025, replayed 60× faster."
2. **Ask the agent (0:30).** On `/agent`, click **"Which borough has the best tippers?"** The 5 suggested questions are `best_tippers`, `rush_hour_distance`, `busiest_zones_last_hour`, `airport_vs_other` and `rejected_why`.
3. **Read the SQL (1:15).** Show the `is_card` filter: cash tips aren't recorded, so the model needed the column comments to get this right.
4. **Trace your ride (1:45).** Open `/ride` on a phone, book a ride, and watch the 7 stages on the phone and on `/trace/live`. It should reach `queryable` in under 5 s.
5. **Close (2:45).** Ask "Where does my ride rank for tips today?"

### Show the plumbing (for curious visitors)
```bash
docker compose exec redpanda rpk topic list                       # 6 topics
docker compose exec redpanda rpk topic consume trips.raw -n 1     # one raw JSON message
```
In ClickHouse:
```bash
docker compose exec clickhouse clickhouse-client -q "select * from taxi.sim_now"
docker compose exec clickhouse clickhouse-client -q "select reason, count() from taxi.trips_rejected group by reason order by 2 desc limit 5"
```
(Check the exact column names in `infra/clickhouse/init/*.sql`.)

### Live controls
```bash
make speed S=120        # more action, then S=60 to go back
make pause / make resume   # avoid pausing in front of visitors
make llm ORDER=internal,local   # switch provider (needs ADMIN_TOKEN in .env)
make reset-rides        # delete visitor rides (asks first)
make logs S=processor   # tail one service
```

---

## Part D: when things go wrong

| Symptom | Fix |
|---|---|
| Dashboard frozen | `make status`, then `make resume`, or `docker compose restart replayer` |
| Agent says "AI is taking a break" | `ollama ps`. Restart `ollama serve`. Suggestion buttons still work from cache |
| Agent slow (> 20 s) | The model is cold. Press a suggestion to warm it |
| Trace stuck at `ingested` | `docker compose restart processor`. It resumes from its offset |
| Phones can't reach `/ride` | Same Wi-Fi? `curl http://<laptop-ip>/ride` from the laptop. Check the macOS firewall |
| Everything odd | Quit Docker Desktop, reopen, `make up` |

---

## Part E: verify and shut down

```bash
make test                # contract + mermaid tests (no Docker)
make eval ONLY=suggested # agent score on the 5 suggested questions (expected 4/5, best_tippers is a known miss)
make down                # stop, keep volumes
```

## Things to know before the real event
- **Docker's VM has about 8 GB.** The containers' memory limits add up to about 8.4 GB, and ClickHouse alone was using 2.7 of its 4 GiB. Raise Docker Desktop's memory to 12 GB or more for the event.
- **Phone access needs the laptop's LAN IP.** The repo expects `STAND_PUBLIC_URL=http://192.168.8.10` (the travel router). This machine currently reports `192.168.57.2` on en0, so update it or the QR code will point to the wrong address.
- **Real-phone check is still open** (M4 status), along with the accepted 4/5 eval. Do the RUNBOOK §3 step 7 real-IP check before opening.
- Fill in the `X out of 32` eval score and the careers/contact placeholders in DEMO_SCRIPT.
