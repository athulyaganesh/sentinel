-- Sentinel schema v1. PostgreSQL is the source of truth.

CREATE TABLE accounts (
    account_id      TEXT PRIMARY KEY,
    tariff_id       TEXT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE devices (
    meter_id            TEXT PRIMARY KEY,
    account_id          TEXT REFERENCES accounts(account_id),
    last_heartbeat_at   TIMESTAMPTZ,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Immutable meter measurements. msg_id is generated ONCE on the device and reused on resend.
CREATE TABLE readings (
    msg_id       TEXT PRIMARY KEY,                 -- idempotency key
    meter_id     TEXT NOT NULL REFERENCES devices(meter_id),
    ts           TIMESTAMPTZ NOT NULL,             -- when the meter took the reading
    kwh          NUMERIC(14,4) NOT NULL CHECK (kwh >= 0),
    received_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX readings_meter_ts ON readings (meter_id, ts);

-- Meaningful state changes only (not every delivery attempt).
CREATE TABLE audit_log (
    id          BIGSERIAL PRIMARY KEY,
    msg_id      TEXT,
    event       TEXT NOT NULL CHECK (event IN ('accepted', 'duplicate_rejected', 'dead_lettered')),
    detail      JSONB,
    at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Messages that could not be processed (malformed, unknown meter, repeated failure).
CREATE TABLE dead_letter (
    id           BIGSERIAL PRIMARY KEY,
    raw_payload  TEXT NOT NULL,
    error        TEXT NOT NULL,
    received_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Versioned time-of-use pricing. Money is integer cents (no floats).
CREATE TABLE tariffs (
    tariff_id              TEXT NOT NULL,
    version                INT  NOT NULL,
    peak_cents_per_kwh     INT  NOT NULL,
    offpeak_cents_per_kwh  INT  NOT NULL,
    peak_start_hour        INT  NOT NULL CHECK (peak_start_hour BETWEEN 0 AND 23),
    peak_end_hour          INT  NOT NULL CHECK (peak_end_hour BETWEEN 1 AND 24),
    effective_from         TIMESTAMPTZ NOT NULL,
    effective_to           TIMESTAMPTZ,            -- NULL = currently active
    PRIMARY KEY (tariff_id, version)
);

-- Posted charges. One entry per account per period (rerunning billing can't double-bill).
CREATE TABLE ledger (
    entry_id        BIGSERIAL PRIMARY KEY,
    account_id      TEXT NOT NULL REFERENCES accounts(account_id),
    period_start    TIMESTAMPTZ NOT NULL,
    period_end      TIMESTAMPTZ NOT NULL,
    kwh             NUMERIC(14,4) NOT NULL,
    amount_cents    BIGINT NOT NULL,
    tariff_id       TEXT NOT NULL,
    tariff_version  INT  NOT NULL,
    posted_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (account_id, period_start, period_end),
    FOREIGN KEY (tariff_id, tariff_version) REFERENCES tariffs(tariff_id, version)
);

-- Corrections to posted charges. Never rewrite the ledger.
CREATE TABLE adjustments (
    adjustment_id    BIGSERIAL PRIMARY KEY,
    ledger_entry_id  BIGINT NOT NULL REFERENCES ledger(entry_id),
    amount_cents     BIGINT NOT NULL,              -- +/- correction
    reason           TEXT NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Seed data
INSERT INTO tariffs VALUES ('residential', 1, 22, 12, 16, 21, '2026-01-01', NULL);
INSERT INTO accounts (account_id, tariff_id) VALUES ('acct-1', 'residential'), ('acct-2', 'residential');
INSERT INTO devices (meter_id, account_id)
SELECT 'meter-' || g, CASE WHEN g % 2 = 0 THEN 'acct-2' ELSE 'acct-1' END
FROM generate_series(1, 100) g;
