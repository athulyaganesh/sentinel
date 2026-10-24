"""Sentinel ingestion service.

Flow per MQTT message:  validate -> DB transaction -> commit -> ACK the broker.
Because we ACK only after commit, a crash or DB outage leaves the message
un-acked, so the broker redelivers it (at-least-once). Duplicates are made
harmless by the UNIQUE msg_id in `readings` (idempotency).
"""
import json
import os
import time
import math 

import paho.mqtt.client as mqtt
import psycopg

MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://sentinel:sentinel@localhost:5432/sentinel")
TOPIC = "meters/+/readings"

conn = None


def get_conn():
    """Return a live DB connection, reconnecting with backoff if Postgres is down."""
    global conn
    delay = 0.5
    while conn is None or conn.closed:
        try:
            conn = psycopg.connect(DATABASE_URL)
        except psycopg.OperationalError as e:
            print(f"DB unavailable ({e}); retrying in {delay:.1f}s", flush=True)
            time.sleep(delay)
            delay = min(delay * 2, 10)
    return conn


def parse(payload: bytes) -> list[dict]:
    """Validate a batch. Return a list of reading dicts, or raise ValueError."""
    try: 
        payload_list = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        raise ValueError(f"Invalid JSON: {e}")
    
    if not isinstance(payload_list, list):
        raise ValueError("Payload is not a list")
    
    if len(payload_list) <= 0: 
        raise ValueError("Empty list") 
    
    now_ms = int(time.time() * 1000)
    max_future_ms = 5 * 60 * 1000 
    
    required_fields = {"msg_id", "meter_id", "ts_ms", "kwh"}
    
    for reading in payload_list: 
        if not isinstance(reading, dict): 
            raise ValueError("Each reading must be a dictionary")
        
        if not required_fields.issubset(reading): 
            raise ValueError("Missing fields in the reading.")
        
        if not isinstance(reading["meter_id"], str)  or not reading["meter_id"] or not reading["meter_id"].strip(): 
            raise ValueError("Meter ID must be non-empty")
        
        if not isinstance(reading["msg_id"], str) or not reading["msg_id"] or not reading["msg_id"].strip():
            raise ValueError("Message ID must be non-empty") 

        ts_ms = reading["ts_ms"]
        kwh = reading["kwh"]
        
        if isinstance(ts_ms, bool) or not isinstance(ts_ms, (int)) or ts_ms < 0 or not math.isfinite(ts_ms): 
            raise ValueError("Time must be an integer or float.")
        if isinstance(kwh, bool) or not isinstance(kwh, (int, float)) or kwh < 0 or not math.isfinite(kwh):
            raise ValueError("KWH must be an integer.")        
        if ts_ms > now_ms + max_future_ms: 
            raise ValueError("Timestamp is too far in the future") 

    return payload_list 
    
def process_batch(readings):
    """Write one batch in a single transaction.

    TODO(you):
      1. INSERT each reading INTO readings ... ON CONFLICT (msg_id) DO NOTHING
      2. If a row was inserted (cursor.rowcount == 1) -> audit_log 'accepted'
         else -> audit_log 'duplicate_rejected'
      3. All of this inside ONE transaction:  with get_conn().transaction(): ...
      Hint: ts_ms -> timestamptz with  to_timestamp(%s / 1000.0)
    """
    raise NotImplementedError


def dead_letter(payload: bytes, error: str):
    # TODO(you): INSERT INTO dead_letter + audit_log 'dead_lettered' (one transaction)
    return 


def on_message(client, userdata, msg):
    try:
        readings = parse(msg.payload)
    except (ValueError, json.JSONDecodeError) as e:
        dead_letter(msg.payload, str(e))      # bad input never crashes the consumer
        client.ack(msg.mid, msg.qos)
        return

    while True:
        try:
            process_batch(readings)
            break
        except psycopg.OperationalError as e:  # DB down: don't ack, retry until it's back
            print(f"DB error, will retry: {e}", flush=True)
            global conn
            conn = None
            get_conn()
    client.ack(msg.mid, msg.qos)              # ACK only after the commit succeeded


def on_connect(client, userdata, flags, reason_code, properties):
    print(f"MQTT connected: {reason_code}", flush=True)
    client.subscribe(TOPIC, qos=1)


def main():
    get_conn()
    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id="sentinel-ingest",
        clean_session=False,   # persistent session: broker queues messages while we're down
        manual_ack=True,       # we decide when to ACK (after DB commit)
    )
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(MQTT_HOST, 1883, keepalive=30)
    client.loop_forever()


if __name__ == "__main__":
    main()
