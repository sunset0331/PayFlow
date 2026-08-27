import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport
from unittest.mock import AsyncMock, patch, MagicMock

from services.gateway.main import app as gateway_app
from services.bank_hdfc.main import app as hdfc_app

# Prevent duplicate metrics registration when importing multiple apps in the same process
from prometheus_client import REGISTRY
collectors = list(REGISTRY._collector_to_names.keys())
for collector in collectors:
    REGISTRY.unregister(collector)

from services.bank_sbi.main import app as sbi_app
from shared.redis_client import get_redis

@pytest_asyncio.fixture
async def gateway_client(mock_asyncpg):
    gateway_app.state.pool = mock_asyncpg
    async with AsyncClient(transport=ASGITransport(app=gateway_app), base_url="http://testserver") as client:
        yield client

@pytest_asyncio.fixture
async def hdfc_client(mock_asyncpg):
    hdfc_app.state.pool = mock_asyncpg
    async with AsyncClient(transport=ASGITransport(app=hdfc_app), base_url="http://testserver") as client:
        yield client

@pytest_asyncio.fixture
async def sbi_client(mock_asyncpg):
    sbi_app.state.pool = mock_asyncpg
    async with AsyncClient(transport=ASGITransport(app=sbi_app), base_url="http://testserver") as client:
        yield client

@pytest_asyncio.fixture(autouse=True)
def mock_redis():
    """Mock Redis globally for all tests."""
    mock = AsyncMock()
    # For SETNX operations (idempotency)
    mock.set.return_value = True
    mock.get.return_value = None
    
    # For ZADD, ZCARD, ZREMRANGEBYSCORE operations (rate limiting)
    mock.zcard.return_value = 0
    mock.zadd.return_value = 1
    mock.zremrangebyscore.return_value = 0
    
    # For INCRBYFLOAT (daily cap)
    mock.incrbyfloat.return_value = 500.0
    
    # Mock the pipeline
    pipe_mock = AsyncMock()
    pipe_mock.execute.return_value = [1, 1, 1]
    mock.pipeline.return_value = pipe_mock
    
    # Patch the global redis_client factory
    with patch("shared.redis_client.get_redis", return_value=mock):
        yield mock

@pytest_asyncio.fixture(autouse=True)
def mock_kafka():
    """Mock AIOKafkaProducer globally."""
    mock_producer = AsyncMock()
    mock_producer.send_and_wait = AsyncMock(return_value=True)
    mock_producer.start = AsyncMock()
    mock_producer.stop = AsyncMock()
    
    with patch("shared.kafka_client.AIOKafkaProducer", return_value=mock_producer):
        yield mock_producer

@pytest_asyncio.fixture(autouse=True)
def mock_asyncpg():
    """Mock PostgreSQL globally."""
    mock_pool = MagicMock()
    mock_conn = MagicMock()
    
    # Setup for transaction and context manager
    acquire_ctx = AsyncMock()
    acquire_ctx.__aenter__.return_value = mock_conn
    mock_pool.acquire.return_value = acquire_ctx
    
    txn_ctx = AsyncMock()
    mock_conn.transaction.return_value = txn_ctx
    
    from decimal import Decimal
    async def smart_fetchrow(query, *args, **kwargs):
        if "FROM transactions" in query:
            return None # Not processed yet
        return {"balance": Decimal("1000.0")}
    
    mock_conn.fetchrow = AsyncMock(side_effect=smart_fetchrow)
    mock_conn.execute = AsyncMock(return_value="UPDATE 1")
    
    with patch("asyncpg.create_pool", return_value=mock_pool):
        yield mock_pool
