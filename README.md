# PayFlow: Distributed UPI-Style Payment System

PayFlow is a production-grade, event-driven microservices architecture that simulates a UPI-style centralized payment gateway. It demonstrates advanced distributed systems concepts including the Saga Choreography pattern, high-concurrency idempotency, and asynchronous event streaming.

## System Architecture

PayFlow consists of 5 independent microservices communicating via HTTP and Apache Kafka, backed by isolated PostgreSQL databases and a Redis caching layer.

```text
User 
 │ (HTTP POST /pay)
 ▼
[ API Gateway (FastAPI) ] ──(Rate Limit & Idempotency)──> [ Redis ]
 │
 ├── [ PostgreSQL: db_gateway ] (Outbox Table)
 │
 └── (Async Poller) ──> [ Apache Kafka: payment.commands ]
                               │
                               ▼
                        [ Payment Worker ]
                         │      │
                         │      ├── 1. Debit/Credit Request (HTTP) ──> [ Bank Services ]
                         │      │
                         │      └── 2. Publish Result ──> [ Apache Kafka: payment.events ]
                         │                                     │
         ┌───────────────────────────────┴───────────────────────────────┐
         ▼                               ▼                               ▼
  [ Gateway Orchestrator ]        [ Ledger Service ]              [ Notification Worker ]
  (Advances Saga State)           (Consumer Group A)              (Consumer Group B)
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

**Absolute Idempotency:** Utilizes Redis `SETNX` (Set if Not Exists) to hash incoming payloads and enforce a 100% duplicate rejection rate within milliseconds, preventing network-retry double-charging.

**Concurrency Control:** Utilizes PostgreSQL `SELECT FOR UPDATE` to lock database rows during balance checks, mathematically preventing race conditions and double-spending.

**Event-Driven Audit Ledger:** Offloads transaction reporting to a background Kafka consumer that appends immutable events to a PostgreSQL ledger.

**Sliding Window Rate Limiter:** Protects the gateway using Redis sorted sets (ZSET) to enforce strict per-minute and per-hour transaction limits per user with <2ms overhead.

**Crash Recovery Worker:** A background asyncio worker in the gateway polls for stuck Sagas (e.g., from network failures or process crashes) using `FOR UPDATE SKIP LOCKED` and correctly advances or compensates their state by querying the banks.

**Distributed Tracing (Structured Logging):** Implements centralized JSON logging across all 5 services using `ServiceLoggerAdapter`. Automatically extracts and propagates a `txn_id` across HTTP and Kafka boundaries for clean observability.

**Expanded Prometheus Metrics:** Comprehensive `/metrics` endpoints instrumented across all services tracking service-level latencies and counters with bounded-cardinality safety (no user-specific labels).

## Project Structure

```plaintext
payflow/
├── docker-compose.yml       # Infrastructure orchestration
├── scripts/
│   └── init-db.sql          # DB initialization & isolation
├── monitoring/
│   └── prometheus.yml       # Metrics scraping config
├── shared/
│   ├── kafka_client.py      # Async publisher
│   ├── rate_limiter.py      # Redis sliding window logic
│   └── redis_client.py      # Connection pooling
└── services/
    ├── gateway/             # Saga Orchestrator & API entry
    ├── bank_hdfc/           # HDFC Bank simulation
    ├── ledger/              # Immutable event consumer
    └── notifications/       # SMS alert consumer
```

## How to Run Locally

### 1. Run via Docker Compose (Recommended)

The easiest way to run the complete PayFlow system is using Docker Compose. This will spin up the Gateway, Banks (HDFC/SBI), Ledger, Notifications, Postgres, Redis, Kafka, and Monitoring (Prometheus/Grafana) all in one go.

```bash
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
### 1. Start the Infrastructure

Spin up PostgreSQL, Redis, Kafka, Zookeeper, Prometheus, and Grafana:

```bash
docker compose up -d
```

### 2. Initialize the Databases

Seed the isolated databases and test accounts:

```bash
docker exec -i payflow-postgres-1 psql -U payflow_admin -d postgres < ./scripts/init-db.sql
```

### 3. Setup Python Environment

```bash
python3 -m venv venv
source venv/bin/activate
pip install fastapi uvicorn redis pydantic aiokafka asyncpg httpx prometheus-client
```

### 4. Run the Microservices

You will need multiple terminal windows (ensure your venv is activated in each).

**Terminal 1 (HDFC Bank):**

```bash
export DATABASE_URL="postgresql://payflow_admin:secretpassword@127.0.0.1:5433/db_bank_hdfc"
uvicorn services.bank_hdfc.main:app --port 8001
```

**Terminal 2 (SBI Bank):**

```bash
export DATABASE_URL="postgresql://payflow_admin:secretpassword@127.0.0.1:5433/db_bank_sbi"
uvicorn services.bank_sbi.main:app --port 8002
```

**Terminal 3 (Ledger Service):**

```bash
uvicorn services.ledger.main:app --port 8003
```

**Terminal 4 (Notification Worker):**

```bash
python -m services.notifications.main
```

**Terminal 5 (API Gateway):**

```bash
uvicorn services.gateway.main:app --port 8000
```

## Testing the Flow

### 1. Successful Payment

```bash
curl -X POST http://127.0.0.1:8000/pay \
  -H "Content-Type: application/json" \
  -d '{"sender_vpa": "utkarsh@hdfc", "receiver_vpa": "alice@sbi", "amount": 500.0}'
```

### 2. Test Idempotency (Duplicate Request)

Run the exact same command above immediately again. It will return `HTTP 409: Duplicate payment detected`.

### 3. Test Saga Compensation (Insufficient Funds)

```bash
curl -X POST http://127.0.0.1:8000/pay \
  -H "Content-Type: application/json" \
  -d '{"sender_vpa": "utkarsh@hdfc", "receiver_vpa": "alice@sbi", "amount": 500000.0}'
```

## Observability

- **Prometheus Metrics:** http://localhost:8000/metrics
- **Grafana Dashboard:** http://localhost:3000 *(Add Prometheus as a data source at `http://prometheus:9090` to visualize P95 latency and throughput)*

## Testing

PayFlow includes a comprehensive `pytest` integration suite that uses `asyncio` to validate high concurrency idempotency, rate-limiting Lua scripts, Saga timeouts, and compensation logic.

### 1. Setup the Test Environment
Ensure you have activated your virtual environment:
```bash
source venv/bin/activate
pip install -r requirements-test.txt  # Or manually install pytest, pytest-asyncio, respx, faker
```

### 2. Run the Test Suite
The tests use mocked asynchronous database connections and a `FakeRedis` implementation to simulate complex distributed failures (like the "Lost Response" scenario and Chaos-injected timeouts):
```bash
pytest tests/ -v
```

**What is tested?**
- **Bank Logic (`test_bank_api.py`)**: Validates Row-Level Locking (Pessimistic Concurrency) and Insufficient Funds logic.
- **Idempotency (`test_gateway_idempotency.py`)**: Simulates 100% identical concurrent requests to guarantee they are deduplicated.
- **Rate Limiting (`test_rate_limiter.py`)**: Tests the Sliding-Window Redis LUA scripts for velocity constraints.
- **Saga Compensation (`test_saga_compensation.py`)**: Validates the Gateway's rollback capability if a receiver bank times out.
- **Failure Injection (`test_failure_injection.py`)**: Chaos testing 503s and Network disconnects.