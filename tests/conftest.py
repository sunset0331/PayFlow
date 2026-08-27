import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport
from unittest.mock import AsyncMock, patch, MagicMock

# Import the FastAPI apps
from services.gateway.main import app as gateway_app
from services.bank_hdfc.main import app as hdfc_app
from services.bank_sbi.main import app as sbi_app
from shared.redis_client import get_redis

@pytest_asyncio.fixture
async def gateway_client():
    async with AsyncClient(transport=ASGITransport(app=gateway_app), base_url="http://testserver") as client:
        yield client

@pytest_asyncio.fixture
async def hdfc_client():
    async with AsyncClient(transport=ASGITransport(app=hdfc_app), base_url="http://testserver") as client:
        yield client

@pytest_asyncio.fixture
async def sbi_client():
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
    mock_pool = AsyncMock()
    mock_conn = AsyncMock()
    
    # Setup for transaction and context manager
    mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
    mock_conn.transaction.return_value.__aenter__.return_value = AsyncMock()
    
    # Standard DB responses
    mock_conn.fetchrow.return_value = {"balance": 10000.0}
    mock_conn.execute.return_value = "UPDATE 1"
    
    with patch("asyncpg.create_pool", return_value=mock_pool):
        yield mock_pool
