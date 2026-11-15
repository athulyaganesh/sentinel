"""Demo data for the AI agent: a billed day for acct-1 plus one late reading."""
import os
import sys
from datetime import datetime, timezone

import psycopg

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "api"))
import billing  # noqa: E402

URL = os.getenv("DATABASE_URL", "postgresql://sentinel:sentinel@localhost:5432/sentinel")


def utc(*a):
    return datetime(*a, tzinfo=timezone.utc)


def add(conn, meter, ts, kwh):
    conn.execute(
        "INSERT INTO readings (msg_id, meter_id, ts, kwh) VALUES (%s, %s, %s, %s) "
        "ON CONFLICT DO NOTHING",
        (f"demo-{meter}-{ts.isoformat()}", meter, ts, kwh),
    )


with psycopg.connect(URL, autocommit=True) as conn:
    for ts, kwh in [(utc(2026, 10, 31, 23), 100), (utc(2026, 11, 1, 15), 105),
                    (utc(2026, 11, 1, 17, 59), 110), (utc(2026, 11, 1, 20, 59), 115),
                    (utc(2026, 11, 1, 23, 59), 118)]:
        add(conn, "meter-1", ts, kwh)

    billing.bill_account(conn, "acct-1", utc(2026, 11, 1), utc(2026, 11, 2))
    add(conn, "meter-1", utc(2026, 11, 1, 23, 59, 30), 120)   # arrives AFTER billing
    print("demo data ready")