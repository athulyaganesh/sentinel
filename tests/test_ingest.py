"""Run with Postgres up:  docker compose up -d postgres && pytest -q"""
import os
import sys

import psycopg
import pytest
from psycopg import errors

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
    with pytest.raises((ValueError)):
        ingest.parse(b"{not valid json")
    ingest.dead_letter(b"{not valid json", "bad json")
    assert count("SELECT count(*) FROM dead_letter") == 1


def reading(msg_id="meter-1-1", meter_id="meter-1", ts_ms=1_761_300_000_000, kwh=0.5):
    return {"msg_id": msg_id, "meter_id": meter_id, "ts_ms": ts_ms, "kwh": kwh}


# ---------- process_batch ----------

def test_timestamp_is_stored_as_the_same_instant():
    ingest.process_batch([reading(ts_ms=1_761_300_000_123)])
    assert count("SELECT (extract(epoch FROM ts) * 1000)::bigint FROM readings") == 1_761_300_000_123


def test_mixed_batch_of_new_and_duplicate_readings():
    ingest.process_batch([reading("meter-1-1")])
    ingest.process_batch([reading("meter-1-1"), reading("meter-1-2")])   # one old, one new
    assert count("SELECT count(*) FROM readings") == 2
    assert count("SELECT count(*) FROM audit_log WHERE event='accepted'") == 2
    assert count("SELECT count(*) FROM audit_log WHERE event='duplicate_rejected'") == 1


def test_unknown_meter_rolls_back_the_whole_batch():
    batch = [reading("meter-1-1"), reading("meter-999-1", meter_id="meter-999")]
    with pytest.raises(errors.ForeignKeyViolation):
        ingest.process_batch(batch)
    # all-or-nothing: the valid reading and its audit row were rolled back too
    assert count("SELECT count(*) FROM readings") == 0
    assert count("SELECT count(*) FROM audit_log") == 0


def test_connection_still_usable_after_a_failed_batch():
    with pytest.raises(errors.ForeignKeyViolation):
        ingest.process_batch([reading("meter-999-1", meter_id="meter-999")])
    ingest.process_batch([reading()])
    assert count("SELECT count(*) FROM readings") == 1


# ---------- dead_letter ----------

def test_dead_letter_stores_payload_error_and_audit_row():
    ingest.dead_letter(b"{not valid json", "bad json")
    assert count("SELECT raw_payload FROM dead_letter") == "{not valid json"
    assert count("SELECT error FROM dead_letter") == "bad json"
    assert count("SELECT count(*) FROM audit_log WHERE event='dead_lettered'") == 1


# ---------- on_message routing (see dead-letter rules) ----------

class FakeClient:
    def __init__(self):
        self.acked = []

    def ack(self, mid, qos):
        self.acked.append(mid)


class FakeMsg:
    def __init__(self, payload, mid=1, qos=1):
        self.payload, self.mid, self.qos = payload, mid, qos


def encode(batch):
    import json
    return json.dumps(batch).encode()


def test_on_message_valid_batch_is_stored_then_acked():
    client = FakeClient()
    ingest.on_message(client, None, FakeMsg(encode([reading()])))
    assert count("SELECT count(*) FROM readings") == 1
    assert client.acked == [1]


def test_on_message_malformed_is_dead_lettered_and_acked():
    client = FakeClient()
    ingest.on_message(client, None, FakeMsg(b"{not valid json"))
    assert count("SELECT count(*) FROM dead_letter") == 1
    assert client.acked == [1]          # permanent failure: ack so the broker stops redelivering


def test_on_message_unknown_meter_is_dead_lettered_and_acked():
    client = FakeClient()
    ingest.on_message(client, None, FakeMsg(encode([reading(meter_id="meter-999")])))
    assert count("SELECT count(*) FROM readings") == 0
    assert count("SELECT count(*) FROM dead_letter") == 1
    assert client.acked == [1]
