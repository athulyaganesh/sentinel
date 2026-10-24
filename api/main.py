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
    return {"ok": True}


@app.get("/latest/{meter_id}")
def latest(meter_id: str):
    # TODO(you): SELECT the newest reading for this meter (ORDER BY ts DESC LIMIT 1); 404 if none.
    raise HTTPException(501, "not implemented")


@app.get("/usage/{meter_id}")
def usage(meter_id: str, start: datetime, end: datetime):
    # TODO(you): readings are a cumulative register, so usage = max(kwh) - min(kwh) in [start, end).
    raise HTTPException(501, "not implemented")


@app.get("/billing/{account_id}")
def billing(account_id: str, start: datetime, end: datetime):
    # TODO(you): hourly usage per meter for the account, priced with the tariff VERSION
    #            effective at each hour (peak vs off-peak). Return kwh + amount_cents (integers!).
    raise HTTPException(501, "not implemented")


@app.get("/reconcile/{account_id}")
def reconcile(account_id: str):
    # TODO(you): recompute expected charges for each ledger entry (+ its adjustments),
    #            compare with posted amount_cents, return discrepancies. Never UPDATE the ledger.
    raise HTTPException(501, "not implemented")
