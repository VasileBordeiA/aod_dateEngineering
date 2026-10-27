# Deploy NYC Taxi Live on Oracle Cloud (Always Free)

This guide puts the whole demo on one free Oracle Cloud server and makes it reachable from the internet. It runs the same Docker Compose stack as the stand laptop, with Google Gemini as the AI (ADR-023, ADR-024).

**What you get:**
- `http://<PUBLIC_IP>/` serves the hub, `/agent`, `/ride`, `/trace/live` and `/grafana/…`.
- Optionally, `https://<your-name>.duckdns.org` with a free certificate (step 8).

**Cost:** $0 on the Always Free allowance. Oracle asks for a card only to verify your identity.

**Time:** about 45 minutes. Most of it is waiting for the first image build.

> Always Free allowance on 2026-10-22: 2 OCPUs, 12 GB RAM and 200 GB block storage, on ARM (Ampere A1). The stack needs about 5 GB of RAM without the local model, and its images already build for ARM, because the stand laptop is Apple silicon.

---

## 1. Create the server (Oracle Console)

1. Sign in at cloud.oracle.com. Check that the region in the top bar is your **home region**, because Always Free resources only exist there.
2. Go to **☰ → Compute → Instances → Create instance**.
3. **Name:** `nyctaxi`.
4. **Image:** click *Change image* → **Ubuntu** → **Canonical Ubuntu 24.04**. Pick the normal image, not "Minimal".
5. **Shape:** click *Change shape* → **Virtual machine** → **Ampere** → **VM.Standard.A1.Flex**.
   - Set **OCPUs = 2** and **Memory = 12 GB**.
   - The shape should show the *Always Free-eligible* label.
6. **Networking:**
   - Choose *Create new virtual cloud network* and *Create new public subnet*. The defaults are fine.
   - Make sure **Automatically assign public IPv4 address** is on.
7. **SSH keys:** choose *Upload public key files (.pub)*.
   - Pick `~/.ssh/id_ed25519.pub` from your Mac.
   - If you don't have one, run `ssh-keygen -t ed25519` first.
8. **Boot volume:** tick *Specify a custom boot volume size* and enter **100 GB**. The free allowance is 200 GB in total. Leave the performance setting at its default.
9. Click **Create**.
   - If you see **"Out of capacity"**, Oracle has no free ARM servers left in that availability domain. Edit *Placement* and try another availability domain, or retry in a few hours.
10. Once it's **Running**, copy the **Public IP address**. This guide calls it `<PUBLIC_IP>`.

## 2. Open ports 80 and 443

Two firewalls need opening: Oracle's, and the one inside the Ubuntu image.

**Oracle's firewall (the subnet's security list):**
1. On the instance page, click the **subnet** link (under *Primary VNIC*).
2. Open **Security** (or *Security Lists*) → **Default Security List** → **Add Ingress Rules**.
3. Enter: Source CIDR `0.0.0.0/0`, IP Protocol **TCP**, Destination Port Range `80,443`. Save.

The firewall inside Ubuntu is step 3.

## 3. Prepare the server

From your Mac:

```bash
ssh ubuntu@<PUBLIC_IP>
```

Then, on the server:

```bash
# Oracle's Ubuntu images block everything except SSH by default: allow web traffic and keep the rule after reboots
sudo iptables -I INPUT 1 -p tcp -m multiport --dports 80,443 -m conntrack --ctstate NEW -j ACCEPT
sudo netfilter-persistent save

# System updates and tools the Makefile uses
sudo apt update && sudo apt -y upgrade
sudo apt -y install make rsync python3 curl

# 4 GB of swap: a safety net for the first image build (the Next.js build is the hungriest step)
sudo fallocate -l 4G /swapfile && sudo chmod 600 /swapfile
sudo mkswap /swapfile && sudo swapon /swapfile
echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab

# Docker Engine + the Compose plugin (official install script)
curl -fsSL https://get.docker.com | sudo sh
sudo usermod -aG docker ubuntu
exit    # log out so the docker group applies
```

Log back in, and check that Docker works without `sudo`:

```bash
ssh ubuntu@<PUBLIC_IP>
docker run --rm hello-world && docker compose version
```

## 4. Upload the project

Run this **on your Mac**, from the project folder. It copies the code, the prepared data (about 180 MB) and your `.env`, and skips build outputs and caches:

```bash
cd ~/Desktop/learning/aod_dataEngineering
rsync -az --progress \
  --exclude .git --exclude .venv --exclude node_modules --exclude .next \
  --exclude __pycache__ --exclude .pytest_cache --exclude .mypy_cache --exclude .ruff_cache \
  --exclude data/raw --exclude data/state \
  ./ ubuntu@92.5.110.68:~/nyctaxi/
```

To update the server after changing code later, run the same command again, then step 6.

## 5. Configure `.env` on the server

```bash
ssh ubuntu@<PUBLIC_IP>
cd ~/nyctaxi
nano .env
```

Change or add these keys, and leave everything else as it is:

```bash
# Public address: printed in the ride QR code
STAND_PUBLIC_URL=http://<PUBLIC_IP>
SITE_ADDRESS=:80

# AI: Gemini only. Neither Ollama nor the company gateway is reachable from Oracle.
LLM_ORDER=gemini
LLM_GEMINI_BASE_URL=https://generativelanguage.googleapis.com/v1beta/openai/
LLM_GEMINI_API_KEY=<your Google AI Studio key>
LLM_GEMINI_MODEL=gemini-2.5-flash
LLM_GEMINI_JSON_MODE=false

# Turn the other providers off, and keep company credentials off a public server
LLM_LOCAL_BASE_URL=
LLM_INTERNAL_BASE_URL=
LLM_INTERNAL_API_KEY=
```

Save with `Ctrl+O`, `Enter`, `Ctrl+X`.

The per-IP rate limits work as designed here: on Linux, Docker passes each visitor's real IP through to Caddy. That's different from Docker Desktop on the Mac (see RUNBOOK).

## 6. Build and start

```bash
cd ~/nyctaxi
make up                                      # first run builds 4 images and pulls the rest: allow 10–20 min
make backfill UNTIL=2025-10-14T07:00:00      # ~7 sim days of history, a few minutes
make status                                  # every container healthy, processor lag near 0
curl -s localhost:8000/api/llm/status        # "active": "gemini"
```

`make up` uses `--wait`, so it returns only after every container reports healthy.

## 7. Try it

Open these from your phone or laptop:

| Page | URL |
|---|---|
| Hub | `http://<PUBLIC_IP>/` |
| Ask the data (agent) | `http://<PUBLIC_IP>/agent` (the header shows **Google Gemini**) |
| Create a ride | `http://<PUBLIC_IP>/ride` |
| Live dashboard | `http://<PUBLIC_IP>/grafana/d/nyc-taxi-live?kiosk&refresh=5s` |
| QR code for the ride page | `http://<PUBLIC_IP>/qr` |

## 8. Optional: a free domain with HTTPS

1. Sign in at **duckdns.org** (GitHub or Google login). Create a subdomain, for example `nyctaxi-demo`, and set its IP to `<PUBLIC_IP>`.
2. On the server, set these in `.env`:

   ```bash
   SITE_ADDRESS=nyctaxi-demo.duckdns.org
   STAND_PUBLIC_URL=https://nyctaxi-demo.duckdns.org
   ```

3. Run `make up`.
   - Caddy gets a Let's Encrypt certificate on its own, which takes a few seconds. It keeps the certificate in the `caddy-data` volume.
   - Plain `http://` requests are redirected to `https://`. The agent's streaming and the trace WebSockets switch to `wss://` automatically.

## Day-to-day

| Task | Command (on the server, in `~/nyctaxi`) |
|---|---|
| See what's running | `make status` |
| Follow one service's log | `make logs S=api` (or `processor`, `replayer`, `caddy`) |
| Stop or start | `make down` / `make up` (data is kept) |
| Switch the AI model | edit `LLM_GEMINI_MODEL` in `.env`, then `make up` |
| Check disk space | `df -h /` and `docker system df` |
| Start over (deletes all data) | `make reset`, `make up`, then the backfill from step 6 |
| Clear visitor rides | `make reset-rides` |

**Disk:** ClickHouse grows by about 3 GB per day at 60×. With a 100 GB disk, check `df -h /` weekly, and run `make reset` when it passes about 70 %.

**Idle reclaim:** Oracle may reclaim an Always Free server that stays idle for 7 days, meaning CPU, network *and* memory all under 20 %. A running stack uses about 40 % of the memory, so leave it running.

## Security notes

- Only Caddy (80/443) and SSH (22) are reachable from the internet. Every other port in `docker-compose.yml` is bound to `127.0.0.1`.
- Caddy answers `/api/admin/*` with 404. `make llm` still works on the server, because it calls the api on `127.0.0.1:8000`.
- The ops Grafana (Stand Health, port 3002) isn't public. Reach it through an SSH tunnel: `ssh -L 3002:127.0.0.1:3002 ubuntu@<PUBLIC_IP>`, then open `http://localhost:3002`.
- The public Grafana is anonymous and read-only (`grafana_ro`), but a visitor can still send it SQL, limited by that user's quota. That's accepted for a demo (ADR-024).
- The Gemini free tier can use prompts to improve Google's products, and its terms limit EEA-facing apps to the paid tier (ADR-023).

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `netfilter-persistent: command not found` (step 3) | The image doesn't ship it | `sudo DEBIAN_FRONTEND=noninteractive apt -y install iptables-persistent`, then `sudo netfilter-persistent save` |
| "Out of capacity" when creating the server | No free ARM capacity in that availability domain | Try another availability domain, or retry later |
| The page doesn't load from outside | Port blocked | Check the security-list rule (step 2), then `sudo iptables -L INPUT -n --line-numbers`: the 80,443 ACCEPT rule must come before any REJECT |
| `make up` stops during the build with "killed" | Out of memory | Check that swap is on (`swapon --show`), then run `make up` again |
| HTTPS doesn't work (step 8) | DuckDNS IP wrong, or port 443 closed | Check that `ping nyctaxi-demo.duckdns.org` shows `<PUBLIC_IP>`, then `make logs S=caddy` |
| The agent says "The AI is taking a short break" | Gemini quota used up (HTTP 429), or a wrong key or model | `make logs S=api \| grep provider_failed`. Check the key and your limits in Google AI Studio. The 5 suggested questions keep working from the cache |
| The header says "cached answers only" | No healthy provider | `curl -s localhost:8000/api/llm/status`. Check the `LLM_GEMINI_*` values, then `make up` |
