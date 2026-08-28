-- Create isolated databases
CREATE DATABASE db_gateway;
CREATE DATABASE db_bank_hdfc;
CREATE DATABASE db_bank_sbi;
CREATE DATABASE db_ledger;

-- Note: In a real setup, we'd switch connections to create tables in specific DBs.
-- For this demonstration script, we will simulate the schemas that the FastAPI 
-- ORM (SQLAlchemy) will generate in their respective databases.

\c db_bank_hdfc;
CREATE TABLE accounts (
    account_id UUID PRIMARY KEY,
    vpa VARCHAR(255) UNIQUE NOT NULL,
    balance DECIMAL(12, 2) NOT NULL CHECK (balance >= 0),
    user_name VARCHAR(255) NOT NULL,
    status VARCHAR(50) DEFAULT 'ACTIVE',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO accounts (account_id, vpa, balance, user_name) VALUES 
('11111111-1111-1111-1111-111111111111', 'utkarsh@hdfc', 50000.0, 'Utkarsh'),
('11111111-1111-1111-1111-222222222222', 'sender@hdfc', 50000.0, 'Sender');

-- id is the surrogate PK; (txn_id, operation_type) enforces idempotency.
-- A debit, credit, or compensation for the same txn_id can each exist once.
CREATE TABLE transactions (
    id             BIGSERIAL PRIMARY KEY,
    txn_id         UUID NOT NULL,
    operation_type VARCHAR(20) NOT NULL,   -- 'DEBIT', 'CREDIT', 'COMPENSATION'
    amount         DECIMAL(12, 2) NOT NULL,
    vpa            VARCHAR(255) NOT NULL,
    status         VARCHAR(50) NOT NULL,
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT txn_operation_unique UNIQUE (txn_id, operation_type)
);

\c db_bank_sbi;
CREATE TABLE accounts (
    account_id UUID PRIMARY KEY,
    vpa VARCHAR(255) UNIQUE NOT NULL,
    balance DECIMAL(12, 2) NOT NULL CHECK (balance >= 0),
    user_name VARCHAR(255) NOT NULL,
    status VARCHAR(50) DEFAULT 'ACTIVE',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO accounts (account_id, vpa, balance, user_name) VALUES 
('22222222-2222-2222-2222-111111111111', 'alice@sbi', 1000.0, 'Alice'),
('22222222-2222-2222-2222-222222222222', 'receiver@sbi', 1000.0, 'Receiver');

-- Same idempotency-safe schema as db_bank_hdfc.
CREATE TABLE transactions (
    id             BIGSERIAL PRIMARY KEY,
    txn_id         UUID NOT NULL,
    operation_type VARCHAR(20) NOT NULL,   -- 'DEBIT', 'CREDIT', 'COMPENSATION'
    amount         DECIMAL(12, 2) NOT NULL,
    vpa            VARCHAR(255) NOT NULL,
    status         VARCHAR(50) NOT NULL,
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT txn_operation_unique UNIQUE (txn_id, operation_type)
);

\c db_gateway;
CREATE TABLE vpa_registry (
    vpa VARCHAR(255) PRIMARY KEY,
    bank_service_url VARCHAR(255) NOT NULL,
    account_id UUID NOT NULL,
    is_active BOOLEAN DEFAULT TRUE
);

INSERT INTO vpa_registry (vpa, bank_service_url, account_id) VALUES 
('utkarsh@hdfc', 'http://bank-hdfc:8001', '11111111-1111-1111-1111-111111111111'),
('sender@hdfc', 'http://bank-hdfc:8001', '11111111-1111-1111-1111-222222222222'),
('alice@sbi', 'http://bank-sbi:8002', '22222222-2222-2222-2222-111111111111'),
('receiver@sbi', 'http://bank-sbi:8002', '22222222-2222-2222-2222-222222222222');

-- Durable saga state for each payment transaction.
-- The gateway persists state at every step so crashes can be recovered.
-- States:
--   INITIATED            -> saga created, not yet processing
--   DEBIT_PENDING        -> debit request sent to sender bank
--   DEBIT_COMPLETED      -> debit confirmed by sender bank
--   CREDIT_PENDING       -> credit request sent to receiver bank
--   COMPENSATING         -> compensation credit being sent to sender bank
--   COMPENSATED          -> compensation confirmed; saga rolled back cleanly
--   COMPLETED            -> end state: payment fully succeeded
--   FAILED               -> end state: payment failed before debit (no money moved)
--   INDETERMINATE        -> end state: credit outcome unknown; needs manual review
--   COMPENSATION_FAILED  -> end state: compensation also failed; needs manual review
CREATE TABLE saga_transactions (
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

CREATE TABLE outbox_events (
    id BIGSERIAL PRIMARY KEY,
    txn_id UUID NOT NULL,
    topic VARCHAR(255) NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMP
);

-- Index for the recovery worker: find stale in-progress sagas efficiently
CREATE INDEX idx_saga_state_updated
    ON saga_transactions (state, updated_at)
    WHERE state NOT IN ('COMPLETED', 'FAILED', 'INDETERMINATE', 'COMPENSATION_FAILED', 'COMPENSATED');

\c db_ledger;
CREATE TABLE events (
    event_id UUID PRIMARY KEY,
    txn_id UUID NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    payload_json JSONB NOT NULL,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
-- Index for fast lookup by transaction ID
CREATE INDEX idx_txn_id ON events(txn_id);