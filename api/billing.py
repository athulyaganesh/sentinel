"""Sentinel billing logic: calculations and DB queries only, no HTTP.

Meters report a cumulative register (an odometer), so usage over any window is
register_before(window_end) - register_before(window_start). Windows are half-open
[start, end) so a reading exactly on a boundary is never counted twice.

v1 decisions (documented limitations):
  * Peak hours are applied in the time zone of the start/end datetimes passed in
    (UTC in practice). Local-time / DST handling is future work.
  * A tariff change in the middle of a billing period is rejected, not split.
  * A meter with no reading before the period start uses a baseline of 0.
"""
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP


def register_before(cur, meter_id: str, t: datetime) -> Decimal:
    """Latest register value strictly before t (0 if the meter has no earlier reading)."""
    cur.execute(
        "SELECT kwh FROM readings WHERE meter_id = %s AND ts < %s ORDER BY ts DESC LIMIT 1",
        (meter_id, t),
    )
    row = cur.fetchone()
    return Decimal(row[0]) if row is not None else Decimal(0)


def split_usage(cur, meter_id: str, start: datetime, end: datetime, tariff: dict) -> tuple[Decimal, Decimal]:
    """Return (peak_kwh, offpeak_kwh) used by one meter in [start, end)."""
    total = register_before(cur, meter_id, end) - register_before(cur, meter_id, start)

    peak = Decimal(0)
    day = start.replace(hour=0, minute=0, second=0, microsecond=0)
    while day < end:
        window_start = day + timedelta(hours=tariff["peak_start_hour"])
        window_end = day + timedelta(hours=tariff["peak_end_hour"])

        # clip the day's peak window to the billing period
        ws = max(window_start, start)
        we = min(window_end, end)
        if ws < we:
            peak += register_before(cur, meter_id, we) - register_before(cur, meter_id, ws)

        day += timedelta(days=1)

    return peak, total - peak


def _tariff_for_period(cur, tariff_id: str, start: datetime, end: datetime) -> dict:
    """The tariff version in effect for the whole of [start, end)."""
    cur.execute(
        """
        SELECT version, peak_cents_per_kwh, offpeak_cents_per_kwh,
               peak_start_hour, peak_end_hour, effective_to
        FROM tariffs
        WHERE tariff_id = %s
          AND effective_from <= %s
          AND (effective_to IS NULL OR effective_to > %s)
        """,
        (tariff_id, start, start),
    )
    t = cur.fetchone()
    if t is None:
        raise ValueError("no tariff in effect for this period")
    if t[5] is not None and t[5] < end:
        raise ValueError("tariff changes mid-period; split the period")
    return {
        "tariff_id": tariff_id,
        "version": t[0],
        "peak_cents": t[1],
        "offpeak_cents": t[2],
        "peak_start_hour": t[3],
        "peak_end_hour": t[4],
    }


def compute_charge(cur, account_id: str, start: datetime, end: datetime) -> dict:
    """What an account owes for [start, end). Read-only: writes nothing.

    Shared by bill_account (post the bill) and reconcile_account (re-check it),
    so both always use the exact same maths.
    """
    if start >= end:
        raise ValueError("start must be before end")

    cur.execute("SELECT tariff_id FROM accounts WHERE account_id = %s", (account_id,))
    row = cur.fetchone()
    if row is None:
        raise ValueError("unknown account")
    tariff = _tariff_for_period(cur, row[0], start, end)

    cur.execute("SELECT meter_id FROM devices WHERE account_id = %s ORDER BY meter_id", (account_id,))
    meters = [r[0] for r in cur.fetchall()]

    peak_total = Decimal(0)
    offpeak_total = Decimal(0)
    for meter_id in meters:
        peak, offpeak = split_usage(cur, meter_id, start, end, tariff)
        peak_total += peak
        offpeak_total += offpeak

    # exact Decimal maths, rounded to whole cents once at the end
    raw = peak_total * tariff["peak_cents"] + offpeak_total * tariff["offpeak_cents"]
    amount_cents = int(raw.quantize(Decimal("1"), rounding=ROUND_HALF_UP))

    return {
        "account_id": account_id,
        "period_start": start,
        "period_end": end,
        "kwh": peak_total + offpeak_total,
        "peak_kwh": peak_total,
        "offpeak_kwh": offpeak_total,
        "amount_cents": amount_cents,
        "tariff_id": tariff["tariff_id"],
        "tariff_version": tariff["version"],
    }


def bill_account(conn, account_id: str, start: datetime, end: datetime) -> dict:
    """Post ONE ledger entry for this account and period. Safe to run more than once.

    ledger has UNIQUE(account_id, period_start, period_end), so a rerun never
    double-bills: it returns the existing entry with already_billed=True.
    """
    with conn.transaction():
        with conn.cursor() as cur:
            charge = compute_charge(cur, account_id, start, end)
            cur.execute(
                """
                INSERT INTO ledger (account_id, period_start, period_end, kwh,
                                    amount_cents, tariff_id, tariff_version)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (account_id, period_start, period_end) DO NOTHING
                RETURNING entry_id, posted_at
                """,
                (account_id, start, end, charge["kwh"], charge["amount_cents"],
                 charge["tariff_id"], charge["tariff_version"]),
            )
            row = cur.fetchone()
            if row is not None:
                return {**charge, "entry_id": row[0], "posted_at": row[1], "already_billed": False}

            # nothing inserted: this period was already billed, return what was posted
            cur.execute(
                """
                SELECT entry_id, kwh, amount_cents, tariff_id, tariff_version, posted_at
                FROM ledger
                WHERE account_id = %s AND period_start = %s AND period_end = %s
                """,
                (account_id, start, end),
            )
            e = cur.fetchone()
            return {
                "account_id": account_id,
                "period_start": start,
                "period_end": end,
                "kwh": e[1],
                "amount_cents": e[2],
                "tariff_id": e[3],
                "tariff_version": e[4],
                "entry_id": e[0],
                "posted_at": e[5],
                "already_billed": True,
            }


def run_billing(conn, start: datetime, end: datetime) -> list[dict]:
    """Bill every account for [start, end). Each account is its own transaction."""
    with conn.cursor() as cur:
        cur.execute("SELECT account_id FROM accounts ORDER BY account_id")
        accounts = [r[0] for r in cur.fetchall()]
    return [bill_account(conn, a, start, end) for a in accounts]


def reconcile_account(cur, account_id: str, start: datetime, end: datetime) -> dict:
    """Check a POSTED bill against current readings. Read-only, never changes the ledger.

    posted   = ledger amount + sum of its adjustments
    expected = compute_charge() run again now
    late_readings = readings inside the period that arrived after the bill was posted
                    (the usual reason for a mismatch).
    """
    cur.execute(
        """
        SELECT entry_id, amount_cents, posted_at FROM ledger
        WHERE account_id = %s AND period_start = %s AND period_end = %s
        """,
        (account_id, start, end),
    )
    entry = cur.fetchone()
    if entry is None:
        raise LookupError("period not billed yet")
    entry_id, ledger_cents, posted_at = entry

    cur.execute(
        "SELECT COALESCE(SUM(amount_cents), 0) FROM adjustments WHERE ledger_entry_id = %s",
        (entry_id,),
    )
    posted = ledger_cents + cur.fetchone()[0]

    expected = compute_charge(cur, account_id, start, end)["amount_cents"]

    cur.execute(
        """
        SELECT COUNT(*) FROM readings r
        JOIN devices d ON d.meter_id = r.meter_id
        WHERE d.account_id = %s
          AND r.ts >= %s AND r.ts < %s
          AND r.received_at > %s
        """,
        (account_id, start, end, posted_at),
    )
    late = cur.fetchone()[0]

    diff = expected - posted
    return {
        "account_id": account_id,
        "entry_id": entry_id,
        "posted_cents": posted,
        "expected_cents": expected,
        "diff_cents": diff,
        "late_readings": late,
        "status": "match" if diff == 0 else "mismatch",
    }