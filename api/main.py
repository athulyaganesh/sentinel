"""Sentinel read API."""
import os
from datetime import datetime
import billing as billing_svc
from billing import register_before
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
        
 
@app.post("/billing/run")
def billing_run(start: datetime, end: datetime):
    """Bill every account for [start, end). Safe to rerun: already-billed periods are not re-charged."""
    if start >= end:
        raise HTTPException(400, "start must be before end")
    with db() as conn:
        try:
            results = billing_svc.run_billing(conn, start, end)
        except ValueError as e:
            raise HTTPException(400, str(e))
    return {
        "billed": [
            {"account_id": r["account_id"], "entry_id": r["entry_id"],
             "amount_cents": r["amount_cents"], "already_billed": r["already_billed"]}
            for r in results
        ]
    }
 
 
@app.get("/reconcile/{account_id}")
def reconcile(account_id: str, start: datetime, end: datetime):
    """Compare the posted bill (ledger + adjustments) with a fresh recomputation. Read-only."""
    if start >= end:
        raise HTTPException(400, "start must be before end")
    with db() as conn:
        with conn.cursor() as cur:
            try:
                return billing_svc.reconcile_account(cur, account_id, start, end)
            except LookupError as e:          # period not billed yet
                raise HTTPException(404, str(e))
            except ValueError as e:           # unknown account, tariff problem
                raise HTTPException(400, str(e))
