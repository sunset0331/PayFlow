# PayFlow: Distributed UPI-Style Payment System

PayFlow is a production-grade, event-driven microservices architecture that simulates a UPI-style centralized payment gateway. It demonstrates advanced distributed systems concepts including the Saga Choreography pattern, high-concurrency idempotency, Transactional Outbox pattern, and asynchronous event streaming.

## System Architecture

PayFlow consists of independent microservices communicating via HTTP and Apache Kafka, backed by isolated PostgreSQL databases and a Redis caching layer.

```text
User / Merchant
 │ (HTTP POST /pay)
 ▼
[ API Gateway (FastAPI) ] ──(Rate Limit & Idempotency)──> [ Redis ]
 │
 ├── [ PostgreSQL: db_gateway ] (Saga State + Outbox Table + DLQ)
 │
 └── (Outbox Poller) ──> [ Apache Kafka: payment_events ]
                                │
                                ▼
  ┌─────────────────────────────┼─────────────────────────────┐
  ▼                             ▼                             ▼
[ Gateway Orchestrator ]    [ Ledger Service ]          [ Notification Worker ]
(Advances Saga State)       (Consumer Group A)          (Consumer Group B)
  │                             │                             │
  └── 1. DB Updates             └── 1. DB Appends             └── 1. Send SMS
  └── 2. Outbox Insert          └── 2. DLQ on failure
  
[ Background Recovery ]
(Polls Gateway DB for stuck Sagas)
  │
  └── (Queries Banks) ──> [ Bank Services (HDFC / SBI) ]
  └── (Compensates/Completes) -> Inserts to Outbox

[ Admin API ]
(HTTP endpoints for resolving INDETERMINATE sagas manually)
  │
  └── Inserts to Outbox & Audit Log
  
[ Reconciliation Worker ] ──(Compares)──> [ Bank Databases ]
(Flags DB discrepancies)  ──(Compares)──> [ Ledger Database ]
```

## Tech Stack

- **Backend:** Python 3.10+, FastAPI, Uvicorn, asyncpg, httpx
- **State & Caching:** Redis (Atomic operations, Sliding Window Rate Limiting)
- **Databases:** PostgreSQL (Database-per-service pattern, Row-level locking)
- **Message Broker:** Apache Kafka, Zookeeper, aiokafka
- **Infrastructure:** Docker, Docker Compose
- **Observability:** Prometheus, Grafana, prometheus-client

## Core Engineering Features

**Distributed Transactions (Saga Pattern):** Bypasses 2-Phase Commit bottlenecks. If a receiver bank fails to credit, the Gateway automatically executes a compensating transaction to refund the sender.

**Transactional Outbox Pattern:** Ensures dual-write safety. When the Gateway or Admin API updates a saga's state in PostgreSQL, it simultaneously inserts an event into an `outbox_events` table in the *same transaction*. A background publisher reliably sends these to Kafka.

**Absolute Idempotency:** Utilizes Redis `SETNX` (Set if Not Exists) to hash incoming payloads and enforce a 100% duplicate rejection rate within milliseconds, preventing network-retry double-charging. Bank endpoints also strictly enforce idempotency.

**Concurrency Control & Race Prevention:** Utilizes PostgreSQL `SELECT FOR UPDATE` and state-guards (`UPDATE ... WHERE state = $expected_state`) to lock database rows during balance checks and saga transitions, mathematically preventing race conditions, duplicate events, and double-spending.

**Event-Driven Audit Ledger:** Offloads transaction reporting to a background Kafka consumer that appends immutable events to a PostgreSQL ledger. 

**Persistent Dead-Letter Queues (DLQ):** Messages that exhaust retry limits in consumers (Orchestrator, Ledger) are routed to persistent `dead_letter_queue` tables in PostgreSQL for manual inspection and replay, surviving container restarts.

**Sliding Window Rate Limiter:** Protects the gateway using Redis sorted sets (ZSET) to enforce strict per-minute and per-hour transaction limits per user with <2ms overhead.

**Crash Recovery Worker:** A background asyncio worker in the gateway polls for stuck Sagas (e.g., from network failures or process crashes) using `FOR UPDATE SKIP LOCKED` and correctly advances or compensates their state by querying the banks.

**HTTP Admin API:** Exposes secure endpoints (`/admin/sagas`) for operators to query and manually resolve `INDETERMINATE` sagas, while ensuring all manual resolutions emit outbox events and maintain ledger parity.

**Distributed Tracing (Structured Logging):** Implements centralized JSON logging across all 5 services using `ServiceLoggerAdapter`. Automatically extracts and propagates a `txn_id` across HTTP and Kafka boundaries for clean observability.

**Automated Reconciliation System:** A separate periodic background worker that safely queries terminal Saga states, Bank APIs, and the Ledger DB using `FOR UPDATE SKIP LOCKED` to cross-reference transactions and reliably flag discrepancies across the distributed microservices.

## Project Structure

```plaintext
payflow/
├── docker-compose.yml       # Infrastructure orchestration
├── .env.example             # Environment variables template
├── scripts/
│   └── init-db.sql          # DB initialization, Outbox & DLQ schemas
├── monitoring/
│   └── prometheus.yml       # Metrics scraping config
├── shared/
│   ├── kafka_client.py      # Async publisher/consumer utilities
│   ├── rate_limiter.py      # Redis sliding window logic
│   └── redis_client.py      # Connection pooling
└── services/
    ├── gateway/             # Saga Orchestrator, Admin API, Recovery & Outbox
    ├── bank_hdfc/           # HDFC Bank simulation
    ├── bank_sbi/            # SBI Bank simulation
    ├── ledger/              # Immutable event consumer & DLQ
    ├── notifications/       # SMS alert consumer
    ├── payment_worker/      # Kafka consumer for payment processing
    └── reconciliation_worker/ # Background anomaly detection
```

## How to Run Locally

### 1. Run via Docker Compose (Recommended)

The easiest way to run the complete PayFlow system is using Docker Compose. This will spin up the Gateway, Banks (HDFC/SBI), Ledger, Notifications, Postgres, Redis, Kafka, and Monitoring (Prometheus/Grafana) all in one go.

```bash
# Setup environment variables
cp .env.example .env

# Start all services
docker compose up --build -d

# Verify services are running
docker compose ps

# Test a payment
curl -X POST http://localhost:8000/pay \
  -H "Content-Type: application/json" \
  -d '{"sender_vpa": "utkarsh@hdfc", "receiver_vpa": "alice@sbi", "amount": 100.0, "currency": "INR"}'

# Stop the system
docker compose down
```

### 2. Run Locally without Docker (Development)

1. Start Infrastructure:
```bash
docker compose up -d postgres redis zookeeper kafka prometheus grafana
```

2. Initialize Databases:
```bash
docker exec -i payflow-postgres-1 psql -U payflow_admin -d postgres < ./scripts/init-db.sql
```

3. Setup Environment:
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

4. Run Microservices (in separate terminals):
```bash
# HDFC Bank
export DATABASE_URL="postgresql://payflow_admin:secretpassword@127.0.0.1:5433/db_bank_hdfc"
uvicorn services.bank_hdfc.main:app --port 8001

# SBI Bank
export DATABASE_URL="postgresql://payflow_admin:secretpassword@127.0.0.1:5433/db_bank_sbi"
uvicorn services.bank_sbi.main:app --port 8002

# Ledger Service
uvicorn services.ledger.main:app --port 8003

# Gateway Service
uvicorn services.gateway.main:app --port 8000
```

## Observability

- **Prometheus Metrics:** `http://localhost:8000/metrics`
- **Grafana Dashboard:** `http://localhost:3000` *(Add Prometheus as a data source at `http://prometheus:9090` to visualize metrics)*

## Testing

PayFlow includes a comprehensive `pytest` integration suite that uses `asyncio` to validate high concurrency idempotency, rate-limiting Lua scripts, Saga timeouts, DLQ logic, and compensation logic.

```bash
pip install -r requirements.txt
pytest tests/ -v
```

The test suite thoroughly covers:
- Saga state machine transitions and strict concurrency guards.
- Bank API idempotency and failure modes.
- Outbox event publishing and Dead-Letter Queue (DLQ) routing.
- Recovery Worker race prevention.
- Redis-based Sliding Window Rate Limiter.
- Admin API resolution constraints.