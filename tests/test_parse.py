"""Unit tests for ingest.parse(). Pure function: no DB or broker needed.

Run:  pytest -q tests/test_parse.py
"""
import json
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ingest"))
import ingest  # noqa: E402

# Freeze "now" so future-timestamp tests are deterministic.
NOW_S = 1_761_300_000            # seconds
NOW_MS = NOW_S * 1000
FIVE_MIN_MS = 5 * 60 * 1000


@pytest.fixture(autouse=True)
def frozen_time(monkeypatch):
    monkeypatch.setattr(ingest.time, "time", lambda: NOW_S)


def reading(**overrides):
    """A valid reading; override fields to make it invalid."""
    r = {"msg_id": "meter-1-1", "meter_id": "meter-1", "ts_ms": NOW_MS, "kwh": 0.5432}
    r.update(overrides)
    return r


def encode(obj) -> bytes:
    return json.dumps(obj).encode()


# ---------- happy paths ----------

def test_valid_batch_returns_readings():
    batch = [reading(), reading(msg_id="meter-1-2", ts_ms=NOW_MS + 1000)]
    assert ingest.parse(encode(batch)) == batch


def test_kwh_can_be_float_or_int():
    batch = [reading(kwh=0.5), reading(msg_id="meter-1-2", kwh=3), reading(msg_id="meter-1-3", kwh=0)]
    assert len(ingest.parse(encode(batch))) == 3


def test_timestamp_exactly_at_future_limit_is_allowed():
    assert ingest.parse(encode([reading(ts_ms=NOW_MS + FIVE_MIN_MS)]))


def test_extra_fields_are_ignored_not_rejected():
    # Design decision: forward-compatible. Newer meters may send extra fields.
    assert ingest.parse(encode([reading(firmware="2.1")]))


# ---------- malformed payloads ----------

@pytest.mark.parametrize("payload", [
    b"{not valid json",
    b"",
    b"\xff\xfe\x00garbage",          # invalid UTF-8
    b'{"msg_id": "x"}',               # object, not a list
    b'"a string"',
    b"42",
    b"null",
    b"[]",                            # empty batch
], ids=["bad-json", "empty-bytes", "bad-utf8", "object", "string", "number", "null", "empty-list"])
def test_rejects_malformed_payload(payload):
    with pytest.raises(ValueError):
        ingest.parse(payload)


@pytest.mark.parametrize("item", ["a string", 5, None, [1, 2]], ids=["str", "int", "null", "list"])
def test_rejects_non_dict_item(item):
    with pytest.raises(ValueError):
        ingest.parse(encode([item]))


@pytest.mark.parametrize("field", ["msg_id", "meter_id", "ts_ms", "kwh"])
def test_rejects_missing_field(field):
    r = reading()
    del r[field]
    with pytest.raises(ValueError):
        ingest.parse(encode([r]))


# ---------- field validation ----------

@pytest.mark.parametrize("field", ["msg_id", "meter_id"])
@pytest.mark.parametrize("bad", ["", "   ", None, 123, True], ids=["empty", "whitespace", "null", "int", "bool"])
def test_rejects_bad_id_fields(field, bad):
    with pytest.raises(ValueError):
        ingest.parse(encode([reading(**{field: bad})]))


@pytest.mark.parametrize("bad", [True, 1.7613e12, "1761300000000", None, NOW_MS + FIVE_MIN_MS + 1, -1],
                         ids=["bool", "float", "string", "null", "too-far-future", "negative"])
def test_rejects_bad_timestamp(bad):
    with pytest.raises(ValueError):
        ingest.parse(encode([reading(ts_ms=bad)]))


@pytest.mark.parametrize("bad", [True, "0.5", None, -0.1], ids=["bool", "string", "null", "negative"])
def test_rejects_bad_kwh(bad):
    with pytest.raises(ValueError):
        ingest.parse(encode([reading(kwh=bad)]))


@pytest.mark.parametrize("raw", [b"NaN", b"Infinity", b"-Infinity"])
def test_rejects_non_finite_kwh(raw):
    # Python's json.loads accepts these by default; json.dumps can't produce them for us.
    payload = b'[{"msg_id":"m-1","meter_id":"m","ts_ms":%d,"kwh":%s}]' % (NOW_MS, raw)
    with pytest.raises(ValueError):
        ingest.parse(payload)


# ---------- batch semantics ----------

def test_one_bad_reading_rejects_whole_batch():
    # Design decision: all-or-nothing. The batch is dead-lettered as a unit.
    batch = [reading(), reading(msg_id="meter-1-2", kwh=-5)]
    with pytest.raises(ValueError):
        ingest.parse(encode(batch))


def test_duplicate_msg_ids_within_batch_are_allowed_here():
    # parse() validates shape only; deduplication is process_batch()'s job (DB UNIQUE).
    batch = [reading(), reading()]
    assert len(ingest.parse(encode(batch))) == 2