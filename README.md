# Sentinel — Real-Time Meter Telemetry & Billing

Simulated smart meters stream readings into a reliable, auditable pipeline that
serves usage and billing, with an AI ops agent that answers questions about meters and bills
using read-only tools.

```
C++ meter simulator ──MQTT QoS1──▶ Mosquitto ──▶ Python ingestion ──▶ PostgreSQL ◀── FastAPI
  stable msg_ids,                 persistent      validate → txn →     readings (immutable)
  batches, dup/junk injection,    sessions,       commit → ACK         audit_log, dead_letter
  clears batch only after ack     bounded queue                        tariffs, ledger, adjustments
```

## Repo layout
| Path | What |
|---|---|
| `simulator/` | C++17 smart-meter simulator (libmosquitto) |
| `ingest/` | Python ingestion service: validate → transaction → commit → ACK, dead-letter routing |
| `api/` | FastAPI API: usage, billing, reconciliation |
| `api/billing.py` | Billing and reconciliation logic (no HTTP) |
| `api/agent_tools.py` | Read-only tools the AI agent can call |
| `api/agent.py` | AI ops agent loop (Gemini, manual tool calling) |
| `scripts/demo_agent_data.py` | Demo data for the agent: a billed day plus one late reading |
| `db/schema.sql` | Schema + seed data (100 meters, 1 tariff) |
| `mosquitto/mosquitto.conf` | Broker: persistence on, bounded queue |
| `tests/` | Unit (`test_parse.py`), integration (`test_ingest.py`), API (`test_api.py`), billing (`test_billing.py`) |
| `.github/workflows/ci.yml` | CI: lint, unit, integration, simulator, docker, end-to-end |

---

## Prerequisites
- Docker Desktop (Docker Compose v2)
- Python 3.12+
- C++17 compiler, CMake, libmosquitto, cJSON
  - **macOS:** `brew install cmake mosquitto cjson`
  - **Ubuntu/Debian:** `sudo apt-get install -y cmake g++ pkg-config libmosquitto-dev libcjson-dev`

## Setup (once)
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r ingest/requirements.txt -r api/requirements.txt pytest httpx
```

---

## Docker: run the stack
```bash
docker compose up -d --build        # Postgres, Mosquitto, ingest, API
docker compose ps                   # all should be running; postgres "healthy"
docker compose logs -f ingest       # watch ingestion
docker compose logs -f api

docker compose stop postgres        # simulate a DB outage
docker compose start postgres       # recover

docker compose down                 # stop everything (data kept)
docker compose down -v              # stop + DELETE data (schema re-runs on next up)
```
`db/schema.sql` runs only when the database volume is first created. After a schema change: `docker compose down -v && docker compose up -d --build`.

## C++ simulator: build and run
```bash
# configure + build (CMake)
cmake -S simulator -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build

# or, the classic way with make
mkdir -p build && cd build && cmake ../simulator && make && cd ..

# strict build (same as CI): warnings are errors
cmake -S simulator -B build -DCMAKE_CXX_FLAGS="-Wall -Wextra -Werror" && cmake --build build
```

```bash
./build/meter_sim --devices 10 --batch-seconds 2 --dup-rate 0.05 &
pkill meter_sim                     # stop it
```
| Flag | Default | Meaning |
|---|---|---|
| `--devices` | 100 | Number of simulated meters |
| `--batch-seconds` | 5 | Readings are sampled every second and published as one batch every N seconds |
| `--dup-rate` | 0.05 | Fraction of batches re-sent with the same `msg_id`s (simulated lost ack) |
| `--host` | localhost | MQTT broker host |

Flags use **two dashes**. Run **one** simulator at a time: all instances share the MQTT client ID `sentinel-simulator`, and the broker disconnects the older client when a second one connects.

## Database: query it
```bash
docker compose exec postgres psql -U sentinel -d sentinel                 # interactive (\q to quit)
docker compose exec postgres psql -U sentinel -d sentinel -c "SELECT count(*) FROM readings;"
```
```sql
-- total vs unique msg_ids (must be equal: no duplicates stored)
SELECT count(*) AS total, count(DISTINCT msg_id) AS unique_ids FROM readings;

-- what ingestion did
SELECT event, count(*) FROM audit_log GROUP BY event;

-- gap check per meter: readings == max sequence number means nothing was lost
SELECT meter_id, count(*) AS readings, max(split_part(msg_id, '-', 3)::int) AS max_seq
FROM readings GROUP BY meter_id ORDER BY meter_id;
```

## API
```bash
curl localhost:8000/health
curl localhost:8000/latest/meter-1
curl "localhost:8000/usage/meter-1?start=2026-10-24T00:00:00Z&end=2026-10-25T00:00:00Z"
```
| Endpoint | Returns | Notes |
|---|---|---|
| `GET /latest/{meter_id}` | `{"kwh": ...}` | Newest reading by timestamp; 404 if none |
| `GET /usage/{meter_id}?start&end` | `{"kwh_used": ...}` | Register at `end` − register at `start` over `[start, end)`; 400 if `start >= end`; 404 if no readings |
| `POST /billing/run?start&end` | `{"billed": [...]}` | Bills every account for `[start, end)`; safe to rerun (`already_billed: true`, never double-bills) |
| `GET /billing/{account_id}?start&end` | `{"total_billed": ...}` | Sum of ledger entries in the range; 404 if none |
| `GET /reconcile/{account_id}?start&end` | posted vs expected | Read-only check of a posted bill; 404 if the period isn't billed |

Interactive docs: http://localhost:8000/docs

## Billing and reconciliation
- **Usage from a cumulative register:** usage over `[start, end)` = register before `end` − register before `start`. Half-open windows, so a boundary reading is never counted twice.
- **Time-of-use pricing:** peak 16:00–21:00 at 22¢/kWh, off-peak 12¢/kWh (tariff `residential` v1). Exact `Decimal` maths, rounded to whole cents once.
- **Idempotent ledger:** `UNIQUE(account_id, period_start, period_end)` + `INSERT ... ON CONFLICT DO NOTHING`; rerunning a billing run returns the existing entry.
- **Corrections are adjustments, never edits:** the ledger is never updated.
- **Reconciliation:** posted (ledger + adjustments) vs a fresh recomputation, plus a count of readings that arrived after the bill was posted.
- **v1 limits:** peak hours applied in UTC; a tariff change mid-period is rejected, not split.

## AI ops agent
Ask questions in plain English; the agent answers using read-only tools over the database.

```
question ──▶ Gemini ──(tool request)──▶ call_tool() ──▶ read-only SQL ──▶ result ──▶ Gemini ──▶ answer
                 ▲________________________ conversation history (resent each round) ______|
```

| Tool | What it does |
|---|---|
| `find_offline_meters(minutes)` | Meters with no reading in the last N minutes (or none ever) |
| `get_bill_status(account_id, start, end)` | Posted vs expected cents, difference, late-reading count |
| `list_late_readings(account_id, start, end)` | Readings that arrived after the bill was posted |
| `get_meter_usage(meter_id, start, end)` | Peak / off-peak kWh for one meter |

**Safety and reliability**
- The model never writes SQL: it can only call the 4 fixed, parameterized tools above (allowlist).
- Read-only connection (`conn.read_only = True`), so Postgres itself rejects any write.
- Automatic function calling is disabled: our code runs each tool, logs it, and records it in `steps`.
- Tool errors go back to the model as data (`{"error": ...}`) instead of crashing the loop.
- Limits: 6 rounds max, 20 s per model call, 5 s per DB query, 45 s overall. On any limit or API error it returns a clear message.

**Run it**
```bash
pip install -r api/requirements.txt
# .env in the repo root (never committed):
#   GEMINI_API_KEY=...
#   GEMINI_MODEL=<a Flash model id, e.g. from client.models.list()>
docker compose up -d postgres
python3 scripts/demo_agent_data.py      # billed day for acct-1 + one late reading
cd api && python3 try_agent.py
```

**Example (2026-11-14, Gemini Flash-Lite)**
> **Q:** Why is acct-1's bill for 2026-11-01 to 2026-11-02 mismatched?
> **A:** 1 late reading arrived after the bill was posted: posted 316¢, expected 340¢, 24¢ short.
> *(tools: `get_bill_status` → `list_late_readings`)*

> **Q:** How much peak vs off-peak energy did meter-1 use on 2026-11-01?
> **A:** 10 kWh peak, 10 kWh off-peak. *(tool: `get_meter_usage`)*

**Lessons so far**
- With failing tools, the model once answered "0 kWh" anyway (hallucination). Prompting alone doesn't prevent this; a code-level guard is next.
- The model read a late reading's register value (120 kWh) as energy used; tools should return clearly named fields (`register_kwh`, `added_kwh`).
- Free-tier 503/504 errors are common; handled gracefully, fallback model planned.

## Tests
```bash
pytest -q tests/test_parse.py                 # unit tests, no DB needed

docker compose up -d postgres                 # integration + API tests need Postgres
pytest -q tests/                              # everything
pytest -q tests/ --cov=ingest --cov=api       # with coverage (pip install pytest-cov)
```

## CI (GitHub Actions)
| Job | Checks |
|---|---|
| lint | Ruff: syntax errors, undefined names, unused imports |
| unit | `test_parse.py` + coverage |
| integration | Postgres 16 service, schema loaded, `test_ingest.py` + `test_api.py` + coverage |
| simulator | C++ build with `-Wall -Wextra -Werror` |
| docker | `docker compose config` + all images build |
| e2e | Full stack + simulator for 20 s with 50% duplicate batches; asserts readings stored, duplicates rejected, no `msg_id` stored twice |

---

## Guarantees (and limits)
- **At-least-once delivery:** MQTT QoS 1 + persistent session; ingestion ACKs only after the DB commit; the simulator clears a batch only after the broker acknowledges it.
- **Idempotent ingestion:** `msg_id` is generated once on the device and is UNIQUE in `readings`; redeliveries are recorded as `duplicate_rejected`.
- **Bad input never crashes the consumer:** malformed messages and unknown meters go to `dead_letter`; DB outages are retried with backoff (0.5 s → 10 s) without ACKing.
- **Limit:** recovery is guaranteed only while the broker's queue stays under `max_queued_messages` (100,000).
- **Billing:** immutable readings, versioned tariffs, idempotent ledger, corrections as `adjustments`; reconciliation reports, never rewrites.

## Results

### DB outage test (manual, 2026-10-24)
10 meters publishing every 2 s; Postgres stopped mid-stream for about a minute, then restarted. Ingestion retried with exponential backoff, then drained the broker's queue.

| Check | Result |
|---|---|
| Total vs unique `msg_id`s | 2,500 / 2,500: no duplicates stored |
| Gap check | All 10 meters: 250 readings, max sequence 250: **no readings lost** |
| `dead_lettered` | 15 (injected malformed messages) |
| `duplicate_rejected` | 1,990 (injected duplicates, plus a second simulator instance accidentally running) |

Observed: while ingest was blocked retrying the DB, its MQTT connection also dropped and reconnected; the persistent session kept the queued messages.

### Load test (manual, 2026-10-24, MacBook + Docker Desktop)
Each meter samples once per second and publishes a 5-reading batch every 5 s (`--dup-rate 0`).
Latency = `received_at - ts` for the newest reading in each batch (so it excludes the intentional batching wait).

| Devices | Readings sent/s | Stored/s | p50 / p95 / p99 latency | Data lost |
|---|---|---|---|---|
| 100 | 100 | ~100 (keeps up) | 93 / 131 / 137 ms | none |
| 1,000 | 1,000 | ~1,060 (keeps up) | 583 / 1,540 / 2,054 ms | none |

At 1,000 devices throughput keeps up, but tail latency rises to ~2 s: every meter publishes on the same tick, so
1,000 batches arrive at once every 5 s and ingest works through them sequentially (thundering herd).
Next: staggered publish offsets, then 10,000 devices to find the throughput ceiling, then bulk inserts if ingest falls behind.

Notes: gap checks taken while counts are still rising can show transient gaps (batches in flight); measure after the
count stops increasing. `ts` comes from the host clock and `received_at` from the Docker VM clock.

## Known limitations / next
- **`msg_id` restarts on simulator restart:** sequence numbers start at 1 each run, so a restarted simulator reuses old `msg_id`s and its new readings are rejected as duplicates. Real meters persist the counter; fix by persisting the sequence or adding a boot ID.
- Shared MQTT client ID across simulator instances (see above).
- Ingest's DB retry blocks the MQTT network thread long enough to trip the keepalive.
- Dev-only credentials are in `docker-compose.yml`; move to `.env` + `.env.example`.
- Agent: no code-level hallucination guard yet; DB errors (`psycopg.Error`) not yet caught in `call_tool`; no row limits on list tools; no fallback model; no `/ask` endpoint or tests yet.
- Not built yet: staggered publish offsets, heartbeats, 10,000-device load test.
