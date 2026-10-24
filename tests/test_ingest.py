"""Run with Postgres up:  docker compose up -d postgres && pytest -q"""
import os
import sys

import psycopg
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ingest"))
import ingest  # noqa: E402

DB = os.getenv("DATABASE_URL", "postgresql://sentinel:sentinel@localhost:5432/sentinel")


@pytest.fixture(autouse=True) # Tests must not depend on each other, so clean up before each test.
def clean():
    with psycopg.connect(DB) as c:
        c.execute("TRUNCATE readings, audit_log, dead_letter RESTART IDENTITY")
    yield


def count(sql):
    with psycopg.connect(DB) as c:
        return c.execute(sql).fetchone()[0]

#idempotency 
def test_duplicate_batch_is_stored_once():
    batch = [{"msg_id": "meter-1-1", "meter_id": "meter-1", "ts_ms": 1_761_300_000_000, "kwh": 0.5}]
    ingest.process_batch(batch)
    ingest.process_batch(batch)            # simulated redelivery
    assert count("SELECT count(*) FROM readings") == 1
    assert count("SELECT count(*) FROM audit_log WHERE event='duplicate_rejected'") == 1

# junk to dead letter 
def test_malformed_payload_goes_to_dead_letter():
    with pytest.raises((ValueError, Exception)):
        ingest.parse(b"{not valid json")
    ingest.dead_letter(b"{not valid json", "bad json")
    assert count("SELECT count(*) FROM dead_letter") == 1


# TODO(stretch): outage test - publish N msgs, `docker compose stop postgres`,
#                     start it again, assert all N msg_ids end up in `readings`.
