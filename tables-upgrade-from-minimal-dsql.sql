-- Upgrade helper for older/minimal project DBs that were created before
-- tables-create-dsql.sql existed.
--
-- Aurora DSQL does not allow some ALTER TABLE ADD COLUMN ... NOT NULL/DEFAULT
-- forms, so this script adds columns in a compatibility-friendly way.

ALTER TABLE accounts ADD COLUMN IF NOT EXISTS external_user_id TEXT;
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS username TEXT;
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ;

ALTER TABLE markets ADD COLUMN IF NOT EXISTS slug TEXT;
ALTER TABLE markets ADD COLUMN IF NOT EXISTS title TEXT;
ALTER TABLE markets ADD COLUMN IF NOT EXISTS description TEXT;
ALTER TABLE markets ADD COLUMN IF NOT EXISTS tick_size_cents BIGINT;
ALTER TABLE markets ADD COLUMN IF NOT EXISTS resolve_time TIMESTAMPTZ;
ALTER TABLE markets ADD COLUMN IF NOT EXISTS created_by UUID;
ALTER TABLE markets ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ;

ALTER TABLE orders ADD COLUMN IF NOT EXISTS reject_reason TEXT;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ;

ALTER TABLE ledger_entries ADD COLUMN IF NOT EXISTS trade_id UUID;
ALTER TABLE ledger_entries ADD COLUMN IF NOT EXISTS cash_txn_id UUID;

CREATE TABLE IF NOT EXISTS trade_parties (
    trade_id UUID NOT NULL,
    account_id UUID NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('BUYER', 'SELLER')),
    side_acquired TEXT NOT NULL CHECK (side_acquired IN ('YES', 'NO')),
    qty BIGINT NOT NULL CHECK (qty > 0),
    cash_delta_cents BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (trade_id, account_id)
);

CREATE INDEX ASYNC IF NOT EXISTS trade_parties_account_idx
    ON trade_parties(account_id);

CREATE TABLE IF NOT EXISTS matchmaker_market_leases (
    market_id UUID PRIMARY KEY,
    owner_id TEXT NOT NULL,
    lease_expires_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX ASYNC IF NOT EXISTS matchmaker_market_leases_exp_idx
    ON matchmaker_market_leases(lease_expires_at);
