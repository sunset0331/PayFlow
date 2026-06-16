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
    status VARCHAR(50) DEFAULT 'ACTIVE'
);

CREATE TABLE transactions (
    txn_id UUID PRIMARY KEY,
    amount DECIMAL(12, 2) NOT NULL,
    type VARCHAR(50) NOT NULL, -- 'DEBIT' or 'CREDIT'
    status VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

\c db_gateway;
CREATE TABLE vpa_registry (
    vpa VARCHAR(255) PRIMARY KEY,
    bank_service_url VARCHAR(255) NOT NULL,
    account_id UUID NOT NULL,
    is_active BOOLEAN DEFAULT TRUE
);

CREATE TABLE payment_requests (
    request_id UUID PRIMARY KEY,
    sender_vpa VARCHAR(255) NOT NULL,
    receiver_vpa VARCHAR(255) NOT NULL,
    amount DECIMAL(12, 2) NOT NULL,
    status VARCHAR(50) NOT NULL, -- 'PENDING', 'SUCCESS', 'FAILED'
    idempotency_key VARCHAR(255) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

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