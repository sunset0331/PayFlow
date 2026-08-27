-- Migration: Add bank transaction idempotency
-- Applies to: db_bank_hdfc and db_bank_sbi
--
-- Problem being fixed:
--   The original schema used txn_id UUID as the PRIMARY KEY on the
--   transactions table. A gateway retry for the same txn_id caused a
--   PK collision -> 500 response -> gateway triggered compensation ->
--   money returned incorrectly (money created out of thin air).
--
-- Fix:
--   Replace the single txn_id PK with a composite UNIQUE(txn_id, operation_type).
--   This allows the same txn_id to have at most one DEBIT, one CREDIT,
--   and one COMPENSATION record -- which is exactly the correct semantic.
--   A duplicate request for the same (txn_id, operation_type) is now
--   detected via ON CONFLICT rather than raising a 500.
--
-- Apply to db_bank_hdfc:
--   docker exec -i <postgres_container> psql -U payflow_admin -d db_bank_hdfc < scripts/migrate_bank_idempotency.sql
--
-- Apply to db_bank_sbi:
--   docker exec -i <postgres_container> psql -U payflow_admin -d db_bank_sbi < scripts/migrate_bank_idempotency.sql

BEGIN;

-- Step 1: Drop the old PRIMARY KEY constraint on txn_id
ALTER TABLE transactions DROP CONSTRAINT IF EXISTS transactions_pkey;

-- Step 2: Add a surrogate auto-increment primary key
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS id BIGSERIAL;
ALTER TABLE transactions ADD CONSTRAINT transactions_pkey PRIMARY KEY (id);

-- Step 3: Add operation_type column if it does not exist
-- Maps to: 'DEBIT', 'CREDIT', 'COMPENSATION'
-- Migrate existing rows: type column -> operation_type
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS operation_type VARCHAR(20);
UPDATE transactions SET operation_type = type WHERE operation_type IS NULL;
ALTER TABLE transactions ALTER COLUMN operation_type SET NOT NULL;

-- Step 4: Add vpa column to record which account was affected
-- Needed for the GET /transaction/{txn_id} query endpoint
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS vpa VARCHAR(255);

-- Step 5: Add the idempotency constraint
ALTER TABLE transactions
    ADD CONSTRAINT txn_operation_unique UNIQUE (txn_id, operation_type);

COMMIT;
