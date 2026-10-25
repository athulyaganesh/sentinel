"""API tests. Run with Postgres up:  docker compose up -d postgres && pytest -q tests/test_api.py
Needs:  pip install fastapi httpx
"""
import os
import sys
from datetime import datetime, timezone

import psycopg
import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "api"))
from main import app  # noqa: E402

DB = os.getenv("DATABASE_URL", "postgresql://sentinel:sentinel@localhost:5432/sentinel")
client = TestClient(app)


@pytest.fixture(autouse=True)
def clean():
    with psycopg.connect(DB) as c:
        c.execute("TRUNCATE readings, audit_log, dead_letter RESTART IDENTITY")
    yield


def t(hh, mm=0, day=24):
    return datetime(2026, 10, day, hh, mm, tzinfo=timezone.utc)


def insert(meter_id, ts, kwh, msg_id=None):
    with psycopg.connect(DB) as c:
        c.execute(
            "INSERT INTO readings (msg_id, meter_id, ts, kwh) VALUES (%s, %s, %s, %s)",
            (msg_id or f"{meter_id}-{ts.isoformat()}", meter_id, ts, kwh),
        )


def usage(meter_id, start, end):
    return client.get(f"/usage/{meter_id}", params={"start": start.isoformat(), "end": end.isoformat()})


# ---------- /latest ----------

def test_latest_returns_newest_reading_by_timestamp_not_insert_order():
    insert("meter-1", t(3), 12.0)
    insert("meter-1", t(5), 13.0)      # newest
    insert("meter-1", t(4), 12.5)      # arrived last, but older (late reading)
    r = client.get("/latest/meter-1")
    assert r.status_code == 200
    body = r.json()
    assert body == {"kwh": pytest.approx(13.0)}   # spec: /latest returns only kwh


def test_latest_only_returns_that_meters_readings():
    insert("meter-1", t(3), 12.0)
    insert("meter-2", t(5), 99.0)
    assert client.get("/latest/meter-1").json()["kwh"] == pytest.approx(12.0)


def test_latest_404_when_meter_has_no_readings():
    assert client.get("/latest/meter-1").status_code == 404


# ---------- /usage ----------

def test_usage_uses_last_reading_before_start_as_baseline():
    # Cumulative register (like an odometer).
    insert("meter-1", t(23, day=23), 10.0)    # before the window -> baseline
    insert("meter-1", t(1), 10.5)
    insert("meter-1", t(5), 11.2)
    r = usage("meter-1", t(0), t(6))
    assert r.status_code == 200
    # Correct: 11.2 - 10.0 = 1.2.  Naive max-min inside the window would give 0.7.
    assert r.json()["kwh_used"] == pytest.approx(1.2)


def test_usage_end_is_exclusive():
    insert("meter-1", t(0), 10.0)
    insert("meter-1", t(6), 15.0)             # exactly at `end` -> not included
    insert("meter-1", t(3), 11.0)
    assert usage("meter-1", t(0, 1), t(6)).json()["kwh_used"] == pytest.approx(1.0)


def test_usage_returns_only_kwh_used():
    insert("meter-1", t(1), 1.0)
    assert set(usage("meter-1", t(0), t(3)).json()) == {"kwh_used"}


def test_usage_is_zero_when_no_new_readings_in_window():
    # Meter has data, just nothing new during the window (idle / no change): 0 used, not 404.
    insert("meter-1", t(1), 10.0)
    r = usage("meter-1", t(2), t(3))
    assert r.status_code == 200
    assert r.json()["kwh_used"] == pytest.approx(0.0)


def test_usage_new_meter_uses_zero_baseline():
    # DECISION: no reading before `start` -> baseline 0 (register starts at 0 on a new meter).
    # If you decide "use the first reading instead", change this test to expect 0.5.
    insert("meter-1", t(1), 0.5)
    insert("meter-1", t(2), 1.0)
    assert usage("meter-1", t(0), t(3)).json()["kwh_used"] == pytest.approx(1.0)


@pytest.mark.parametrize("start,end", [(t(3), t(3)), (t(4), t(3))], ids=["equal", "reversed"])
def test_usage_400_when_start_not_before_end(start, end):
    assert usage("meter-1", start, end).status_code == 400


def test_usage_404_when_meter_has_no_readings_at_all():
    assert usage("meter-1", t(0), t(3)).status_code == 404


def test_usage_422_on_bad_datetime():
    r = client.get("/usage/meter-1", params={"start": "yesterday", "end": "today"})
    assert r.status_code == 422          # FastAPI validation
