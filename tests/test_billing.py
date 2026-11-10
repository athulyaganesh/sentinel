"""Integration tests for billing.py. Needs the Sentinel Postgres (schema + seed loaded).
 
Tariff 'residential' v1: peak 22c/kWh 16:00-21:00, off-peak 12c/kWh. acct-1 owns odd meters.
"""
import os
import sys
from datetime import datetime, timezone
 
import psycopg
import pytest
 
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "api"))
import billing  # noqa: E402
 
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://sentinel:sentinel@localhost:5432/sentinel")
 
 
def utc(*args):
    return datetime(*args, tzinfo=timezone.utc)
 
 
DAY_START = utc(2026, 11, 1)
DAY_END = utc(2026, 11, 2)
 
 
@pytest.fixture
def conn():
    with psycopg.connect(DATABASE_URL, autocommit=True) as c:
        c.execute("TRUNCATE adjustments, ledger, readings RESTART IDENTITY CASCADE")
        c.execute("UPDATE tariffs SET effective_to = NULL WHERE tariff_id = 'residential' AND version = 1")
        yield c
        c.execute("TRUNCATE adjustments, ledger, readings RESTART IDENTITY CASCADE")
        c.execute("UPDATE tariffs SET effective_to = NULL WHERE tariff_id = 'residential' AND version = 1")
 
 
def add_readings(conn, meter_id, points):
    """points: list of (ts, cumulative_kwh)."""
    for i, (ts, kwh) in enumerate(points):
        conn.execute(
            "INSERT INTO readings (msg_id, meter_id, ts, kwh) VALUES (%s, %s, %s, %s)",
            (f"{meter_id}-{ts.isoformat()}-{i}", meter_id, ts, kwh),
        )
 
 
def standard_day(conn, meter_id):
    """Uses 5 kWh off-peak before 16:00, 5 more before 18:00 (peak), 10 total peak, 3 off-peak after.
 
    total 18 kWh, peak 10 kWh, off-peak 8 kWh -> 10*22 + 8*12 = 316 cents.
    """
    add_readings(conn, meter_id, [
        (utc(2026, 10, 31, 23, 0), 100),   # baseline before the period
        (utc(2026, 11, 1, 15, 0), 105),    # off-peak usage: 5
        (utc(2026, 11, 1, 17, 59), 110),   # inside peak
        (utc(2026, 11, 1, 20, 59), 115),   # peak usage so far: 10
        (utc(2026, 11, 1, 23, 59), 118),   # off-peak after peak: 3
    ])
 
 
def test_peak_and_offpeak_priced_separately(conn):
    standard_day(conn, "meter-1")
    with conn.cursor() as cur:
        peak, offpeak = billing.split_usage(cur, "meter-1", DAY_START, DAY_END,
                                            {"peak_start_hour": 16, "peak_end_hour": 21})
        charge = billing.compute_charge(cur, "acct-1", DAY_START, DAY_END)
    assert (peak, offpeak) == (10, 8)
    assert charge["amount_cents"] == 316
 
 
def test_period_starting_mid_day_clips_peak_window(conn):
    standard_day(conn, "meter-1")
    with conn.cursor() as cur:
        charge = billing.compute_charge(cur, "acct-1", utc(2026, 11, 1, 18), DAY_END)
    # from 18:00: total 118-110 = 8, peak 115-110 = 5, off-peak 3 -> 5*22 + 3*12
    assert (charge["peak_kwh"], charge["offpeak_kwh"]) == (5, 3)
    assert charge["amount_cents"] == 146
 
 
def test_multiple_meters_add_up(conn):
    standard_day(conn, "meter-1")
    standard_day(conn, "meter-3")
    with conn.cursor() as cur:
        assert billing.compute_charge(cur, "acct-1", DAY_START, DAY_END)["amount_cents"] == 632
 
 
def test_other_accounts_meters_not_billed(conn):
    standard_day(conn, "meter-2")              # belongs to acct-2
    with conn.cursor() as cur:
        assert billing.compute_charge(cur, "acct-1", DAY_START, DAY_END)["amount_cents"] == 0
 
 
def test_billing_twice_does_not_double_bill(conn):
    standard_day(conn, "meter-1")
    first = billing.bill_account(conn, "acct-1", DAY_START, DAY_END)
    second = billing.bill_account(conn, "acct-1", DAY_START, DAY_END)
    assert first["already_billed"] is False
    assert second["already_billed"] is True
    assert second["entry_id"] == first["entry_id"]
    assert second["amount_cents"] == 316
    assert conn.execute("SELECT COUNT(*) FROM ledger").fetchone()[0] == 1
 
 
def test_run_billing_bills_every_account(conn):
    standard_day(conn, "meter-1")
    results = billing.run_billing(conn, DAY_START, DAY_END)
    assert {r["account_id"]: r["amount_cents"] for r in results} == {"acct-1": 316, "acct-2": 0}
 
 
def test_reconcile_matches_when_nothing_changed(conn):
    standard_day(conn, "meter-1")
    billing.bill_account(conn, "acct-1", DAY_START, DAY_END)
    with conn.cursor() as cur:
        r = billing.reconcile_account(cur, "acct-1", DAY_START, DAY_END)
    assert (r["status"], r["diff_cents"], r["late_readings"]) == ("match", 0, 0)
 
 
def test_late_reading_causes_mismatch_and_adjustment_fixes_it(conn):
    standard_day(conn, "meter-1")
    entry = billing.bill_account(conn, "acct-1", DAY_START, DAY_END)
 
    # a reading for the billed day arrives after posting: 2 more off-peak kWh = 24 cents
    add_readings(conn, "meter-1", [(utc(2026, 11, 1, 23, 59, 30), 120)])
    with conn.cursor() as cur:
        r = billing.reconcile_account(cur, "acct-1", DAY_START, DAY_END)
    assert (r["status"], r["diff_cents"], r["late_readings"]) == ("mismatch", 24, 1)
 
    # correct it the right way: an adjustment, never an UPDATE to the ledger
    conn.execute(
        "INSERT INTO adjustments (ledger_entry_id, amount_cents, reason) VALUES (%s, %s, %s)",
        (entry["entry_id"], 24, "late reading"),
    )
    with conn.cursor() as cur:
        r = billing.reconcile_account(cur, "acct-1", DAY_START, DAY_END)
    assert (r["status"], r["posted_cents"]) == ("match", 340)
    assert conn.execute("SELECT amount_cents FROM ledger").fetchone()[0] == 316   # ledger untouched
 
 
def test_tariff_change_mid_period_rejected(conn):
    conn.execute("UPDATE tariffs SET effective_to = %s WHERE tariff_id = 'residential' AND version = 1",
                 (utc(2026, 11, 1, 12),))
    with conn.cursor() as cur, pytest.raises(ValueError, match="mid-period"):
        billing.compute_charge(cur, "acct-1", DAY_START, DAY_END)
 
 
def test_unknown_account_and_unbilled_period(conn):
    with conn.cursor() as cur:
        with pytest.raises(ValueError, match="unknown account"):
            billing.compute_charge(cur, "acct-nope", DAY_START, DAY_END)
        with pytest.raises(LookupError):
            billing.reconcile_account(cur, "acct-1", DAY_START, DAY_END)
