"""
Read-only tools for the AI ops agent.

Rules: no writes, no raw SQL from the model.
Each tool is a fixed, parameterized query.
"""

from datetime import datetime, timezone
from decimal import Decimal

import billing


def _ts(value) -> datetime:
    """Accept a datetime or an ISO string (what the model sends). Always return UTC-aware."""
    t = value if isinstance(value, datetime) else datetime.fromisoformat(value)
    return t if t.tzinfo else t.replace(tzinfo=timezone.utc)


def _clean(obj):
    """Make results JSON-safe: Decimal -> float, datetime -> ISO string."""
    if isinstance(obj, dict):
        return {k: _clean(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_clean(v) for v in obj]
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, datetime):
        return obj.isoformat()
    return obj


def _check_period(start: datetime, end: datetime) -> None:
    if start >= end:
        raise ValueError("start must be before end")


def find_offline_meters(cur, minutes: int = 30) -> list[dict]:
    """Meters with no reading in the last `minutes`, or no readings at all."""
    cur.execute(
        """
        SELECT d.meter_id, d.account_id, MAX(r.ts) AS last_seen
        FROM devices d
        LEFT JOIN readings r ON r.meter_id = d.meter_id
        GROUP BY d.meter_id, d.account_id
        HAVING MAX(r.ts) IS NULL
            OR MAX(r.ts) < now() - make_interval(mins => %s)
        ORDER BY last_seen NULLS FIRST, d.meter_id
        """,
        (minutes,),
    )
    return _clean([
        {"meter_id": meter_id, "account_id": account_id, "last_seen": last_seen}
        for meter_id, account_id, last_seen in cur.fetchall()
    ])


def get_bill_status(cur, account_id, start, end) -> dict:
    """Posted vs expected amount for a billed period, plus how many readings arrived late."""
    start, end = _ts(start), _ts(end)
    _check_period(start, end)
    return _clean(billing.reconcile_account(cur, account_id, start, end))


def list_late_readings(cur, account_id, start, end) -> list[dict]:
    """Readings inside a billed period that arrived after the bill was posted."""
    start, end = _ts(start), _ts(end)
    _check_period(start, end)

    cur.execute(
        """
        SELECT posted_at FROM ledger
        WHERE account_id = %s AND period_start = %s AND period_end = %s
        """,
        (account_id, start, end),
    )
    row = cur.fetchone()
    if row is None:
        raise LookupError("period not billed yet")
    posted_at = row[0]

    cur.execute(
        """
        SELECT r.meter_id, r.ts, r.kwh, r.received_aT
        FROM readings r
        JOIN devices d ON d.meter_id = r.meter_id
        WHERE d.account_id = %s
          AND r.ts >= %s AND r.ts < %s
          AND r.received_at > %s                              
        ORDER BY r.received_at
        """,
        (account_id, start, end, posted_at),
    )
    return _clean([
        {"meter_id": m, "ts": ts, "kwh": kwh, "received_at": received_at}
        for m, ts, kwh, received_at in cur.fetchall()
    ])


def get_meter_usage(cur, meter_id, start, end) -> dict:
    """Peak and off-peak kWh for one meter in [start, end), using its account's tariff."""
    start, end = _ts(start), _ts(end)
    _check_period(start, end)                                 

    cur.execute(
        """
        SELECT a.tariff_id
        FROM devices d
        JOIN accounts a ON a.account_id = d.account_id
        WHERE d.meter_id = %s
        """,
        (meter_id,),
    )
    row = cur.fetchone()
    if row is None:
        raise ValueError("unknown meter")

    tariff = billing._tariff_for_period(cur, row[0], start, end)
    peak, offpeak = billing.split_usage(cur, meter_id, start, end, tariff)
    return _clean({
        "meter_id": meter_id,
        "period_start": start,
        "period_end": end,
        "peak_kwh": peak,
        "offpeak_kwh": offpeak,
        "total_kwh": peak + offpeak,
    })