"""Sentinel read API."""
import os
from datetime import datetime

import psycopg
from fastapi import FastAPI, HTTPException

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://sentinel:sentinel@localhost:5432/sentinel")
app = FastAPI(title="Sentinel")


def db():
    return psycopg.connect(DATABASE_URL)


@app.get("/health")
def health():
    return {"ok": True} #TODO: maybe check DB connection too? 


@app.get("/latest/{meter_id}")
def latest(meter_id: str):
    with db() as conn: 
        with conn.cursor() as cur: 
            cur.execute(
                """
                SELECT kwh FROM readings where meter_id = %s ORDER BY ts DESC LIMIT 1
                """, 
                (meter_id,),
            )
            row = cur.fetchone()

            if row is None:  # zero rows -> fetchone() is None
                raise HTTPException(404, "No readings from this meter")

            return {"kwh": float(row[0])}


def register_before(cur, meter_id: str, t: datetime) -> float:
    """Meter's running total (register) from the last reading strictly before t; 0 if none."""
    cur.execute(
        """
        SELECT kwh FROM readings
        WHERE meter_id = %s AND ts < %s
        ORDER BY ts DESC
        LIMIT 1
        """,
        (meter_id, t),
    )
    row = cur.fetchone()
    return float(row[0]) if row is not None else 0.0


@app.get("/usage/{meter_id}")
def usage(meter_id: str, start: datetime, end: datetime):
    if start >= end:
        raise HTTPException(400, "start must be before end")

    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM readings WHERE meter_id = %s LIMIT 1", (meter_id,))
            if cur.fetchone() is None:
                raise HTTPException(404, "No readings from this meter")

            # Readings are a cumulative register: usage in [start, end) = total at end - total at start.
            kwh_used = register_before(cur, meter_id, end) - register_before(cur, meter_id, start)
            return {"kwh_used": kwh_used}


@app.get("/billing/{account_id}")
def billing(account_id: str, start: datetime, end: datetime):
    if start >= end: 
        raise HTTPException(400, "start must be before end")
    
    with db() as conn: 
        with conn.cursor() as cur: 
            cur.execute( 
            
                """
                SELECT SUM(amount_cents) as total_billed
                FROM ledger 
                WHERE account_id =  %s 
                AND period_start >= %s
                AND period_end <= %s
                """, 
                (
                    account_id, 
                    start,
                    end 
                ),
            )
            row = cur.fetchone() 
            
            if row[0] is None: #sum 
                raise HTTPException(404, "No ledger entries for this account in the given time range")
            return {"total_billed": row[0]} 
            

@app.get("/reconcile/{account_id}")
def reconcile(account_id: str):
    # TODO: recompute expected charges for each ledger entry (+ its adjustments),
    #            compare with posted amount_cents, return discrepancies. Never UPDATE the ledger.
    raise HTTPException(501, "not implemented")
