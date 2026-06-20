#!/usr/bin/env bash
set -euo pipefail

DB_URL="${DATABASE_URL:-postgresql://flashsale:flashsale@localhost:5432/flashsale}"

echo "==> Seeding PostgreSQL with user wallet data and schema..."

SQL=$(cat <<'EOSQL'
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE IF NOT EXISTS user_wallets (
    user_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email          VARCHAR(255) NOT NULL UNIQUE,
    display_name   VARCHAR(100) NOT NULL,
    balance_cents  INTEGER NOT NULL DEFAULT 0 CHECK (balance_cents >= 0),
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS idempotency_store (
    idempotency_key   UUID PRIMARY KEY,
    transaction_id    UUID NOT NULL,
    status            VARCHAR(20) NOT NULL DEFAULT 'PROCESSING'
                      CHECK (status IN ('PROCESSING', 'COMPLETED', 'FAILED')),
    response_payload  JSONB,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at        TIMESTAMPTZ NOT NULL DEFAULT (NOW() + INTERVAL '24 hours')
);

CREATE INDEX IF NOT EXISTS idx_idempotency_transaction ON idempotency_store(transaction_id);

CREATE TABLE IF NOT EXISTS payment_ledger (
    ledger_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    transaction_id  UUID NOT NULL,
    user_id         UUID NOT NULL,
    event_id        UUID NOT NULL,
    operation       VARCHAR(20) NOT NULL CHECK (operation IN ('CHARGE', 'REFUND')),
    amount_cents    INTEGER NOT NULL CHECK (amount_cents > 0),
    currency        VARCHAR(3) NOT NULL DEFAULT 'USD',
    status          VARCHAR(20) NOT NULL CHECK (status IN ('SUCCESS', 'FAILED', 'PENDING')),
    description     TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ledger_transaction ON payment_ledger(transaction_id);
CREATE INDEX IF NOT EXISTS idx_ledger_user ON payment_ledger(user_id);

INSERT INTO user_wallets (user_id, email, display_name, balance_cents) VALUES
    ('aaaaaaaa-1111-1111-1111-111111111111', 'alice@example.com', 'Alice', 500000),
    ('bbbbbbbb-2222-2222-2222-222222222222', 'bob@example.com',   'Bob',   250000),
    ('cccccccc-3333-3333-3333-333333333333', 'carol@example.com', 'Carol', 750000),
    ('dddddddd-4444-4444-4444-444444444444', 'dave@example.com',  'Dave',  100000),
    ('eeeeeeee-5555-5555-5555-555555555555', 'eve@example.com',   'Eve',   150000)
ON CONFLICT (email) DO NOTHING;

EOSQL
)

echo "$SQL" | psql "$DB_URL" -q
echo "PostgreSQL seeded: 5 test users with wallets."
echo "  Alice:  \$5,000.00"
echo "  Bob:    \$2,500.00"
echo "  Carol:  \$7,500.00"
echo "  Dave:   \$1,000.00"
echo "  Eve:    \$1,500.00"
