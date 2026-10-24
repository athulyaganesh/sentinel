# Sentinel — Real-Time Meter Telemetry & Billing

Simulated smart meters stream readings into a reliable, auditable pipeline that
serves usage and billing.

```
C++ meter simulator ──MQTT QoS1──▶ Mosquitto ──▶ Python ingestion ──▶ PostgreSQL ◀── FastAPI
  stable msg_ids,                 persistent      validate → txn →     readings (immutable)
  batches, dup/junk injection     sessions,       commit → ACK         audit_log, dead_letter
                                  bounded queue                        tariffs, ledger, adjustments
```

## Run
```bash
docker compose up -d --build
cmake -S simulator -B build && cmake --build build   # needs libmosquitto-dev
./build/meter_sim --devices 100 --batch-seconds 5 --dup-rate 0.05
curl localhost:8000/latest/meter-1
pytest -q
```

## Guarantees (and limits)
- **At-least-once delivery** (MQTT QoS 1 + persistent session + ACK after DB commit).
- **Idempotent ingestion:** `msg_id` is generated once on the device and is UNIQUE in `readings`; redeliveries are recorded as `duplicate_rejected`.
- **Bad input never crashes the consumer:** malformed messages go to `dead_letter`.
- **Billing is auditable:** readings are immutable, tariffs are versioned, corrections are `adjustments`; reconciliation reports, never rewrites.
- **Limits:** recovery from a DB/consumer outage is guaranteed only while the broker's queue stays under `max_queued_messages` (100,000). <!-- TODO: document measured behavior -->

## Results
<!-- TODO: load-test numbers (msgs/sec, p95/p99 latency, recovery time) — real measurements only -->
