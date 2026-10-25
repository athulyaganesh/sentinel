# Sentinel — Real-Time Meter Telemetry & Billing

Simulated smart meters stream readings into a reliable, auditable pipeline that
serves usage and billing.

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
| `api/` | FastAPI read API (`/latest`, `/usage`) |
| `db/schema.sql` | Schema + seed data (100 meters, 1 tariff) |
| `mosquitto/mosquitto.conf` | Broker: persistence on, bounded queue |
| `tests/` | Unit (`test_parse.py`), integration (`test_ingest.py`), API (`test_api.py`) |
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

Interactive docs: http://localhost:8000/docs

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
- **Billing (planned):** immutable readings, versioned tariffs, corrections as `adjustments`; reconciliation reports, never rewrites.

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

### Load test
<!-- TODO: msgs/sec, p95/p99 latency, recovery time at 100 / 1,000 / 10,000 devices — real measurements only -->

## Known limitations / next
- **`msg_id` restarts on simulator restart:** sequence numbers start at 1 each run, so a restarted simulator reuses old `msg_id`s and its new readings are rejected as duplicates. Real meters persist the counter; fix by persisting the sequence or adding a boot ID.
- Shared MQTT client ID across simulator instances (see above).
- Ingest's DB retry blocks the MQTT network thread long enough to trip the keepalive.
- Dev-only credentials are in `docker-compose.yml`; move to `.env` + `.env.example`.
- Not built yet: staggered publish offsets, billing/reconcile, heartbeats, load tests.
