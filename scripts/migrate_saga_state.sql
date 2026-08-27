-- Migration: Add durable saga state to db_gateway
-- Applies to: db_gateway
--
-- Problem being fixed:
--   The gateway held all saga state (txn_id, step, outcome) in local
--   Python variables inside the HTTP request handler. A process crash
--   after the debit and before the credit left money stuck permanently:
--   the txn_id was lost and no recovery was possible.
--
-- Fix:
--   Create saga_transactions to persist state at each saga step.
--   The gateway now writes DEBIT_PENDING before calling the bank,
--   DEBIT_COMPLETED after confirmation, etc.
--
-- Apply:
--   docker exec -i <postgres_container> psql -U payflow_admin -d db_gateway < scripts/migrate_saga_state.sql

BEGIN;

-- Drop the old unused ghost table (was defined in schema but never used by code)
DROP TABLE IF EXISTS payment_requests;

-- Create the durable saga state table
CREATE TABLE IF NOT EXISTS saga_transactions (
    saga_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    txn_id          UUID UNIQUE NOT NULL,
    sender_vpa      VARCHAR(255) NOT NULL,
    receiver_vpa    VARCHAR(255) NOT NULL,
    amount          DECIMAL(12,2) NOT NULL,
    state           VARCHAR(30) NOT NULL DEFAULT 'INITIATED',
    idempotency_key VARCHAR(255) UNIQUE,
    error_reason    TEXT,
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW()
);

-- Index for the recovery worker: find stale in-progress sagas quickly
CREATE INDEX IF NOT EXISTS idx_saga_state_updated
    ON saga_transactions (state, updated_at)
    WHERE state NOT IN ('COMPLETED', 'FAILED', 'INDETERMINATE', 'COMPENSATION_FAILED', 'COMPENSATED');

COMMIT;
