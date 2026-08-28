import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport
from unittest.mock import AsyncMock, patch, MagicMock

from services.gateway.main import app as gateway_app
from services.bank_hdfc.main import app as hdfc_app

# Prevent duplicate metrics registration when importing multiple apps in the same process
from prometheus_client import REGISTRY
# No longer needed since bank metrics are shared

from services.bank_sbi.main import app as sbi_app
from shared.redis_client import get_redis

@pytest_asyncio.fixture
async def gateway_client(mock_asyncpg, mock_redis):
    gateway_app.state.db_pool = mock_asyncpg
    gateway_app.dependency_overrides[get_redis] = lambda: mock_redis
    async with AsyncClient(transport=ASGITransport(app=gateway_app), base_url="http://testserver") as client:
        yield client
        gateway_app.dependency_overrides.clear()

@pytest_asyncio.fixture
async def hdfc_client(mock_asyncpg, mock_redis):
    hdfc_app.state.pool = mock_asyncpg
    hdfc_app.dependency_overrides[get_redis] = lambda: mock_redis
    async with AsyncClient(transport=ASGITransport(app=hdfc_app), base_url="http://testserver") as client:
        yield client
        hdfc_app.dependency_overrides.clear()

@pytest_asyncio.fixture
async def sbi_client(mock_asyncpg, mock_redis):
    sbi_app.state.pool = mock_asyncpg
    sbi_app.dependency_overrides[get_redis] = lambda: mock_redis
    async with AsyncClient(transport=ASGITransport(app=sbi_app), base_url="http://testserver") as client:
        yield client
        sbi_app.dependency_overrides.clear()

@pytest_asyncio.fixture(autouse=True)
def mock_redis():
    """Stateful Mock Redis globally for all tests."""
    store = {}
    zstore = {}
    
    class FakePipeline:
        async def execute(self):
            return [1, 1, 1]
            
    class FakeRedis:
        async def get(self, key):
            return store.get(key)
            
        async def set(self, key, value, nx=False, ex=None):
            if nx and key in store:
                return False
            store[key] = value
            return True
            
        async def zcard(self, key):
            return len(zstore.get(key, []))
            
        async def zadd(self, key, mapping):
            if key not in zstore:
                zstore[key] = []
            for name, score in mapping.items():
                zstore[key].append(score)
            return 1
            
        async def zremrangebyscore(self, key, min_val, max_val):
            return 0
            
        async def incrbyfloat(self, key, amount):
            store[key] = store.get(key, 0.0) + amount
            return store[key]
            
        def pipeline(self):
            return FakePipeline()
            
    mock = FakeRedis()
    
    # Patch the global redis_client factory
    with patch("shared.redis_client.get_redis", return_value=mock):
        yield mock

@pytest_asyncio.fixture(autouse=True)
def mock_kafka():
    """Mock Kafka producer globally."""
    producer_mock = AsyncMock()
    producer_mock.send_and_wait = AsyncMock(return_value=True)
    with patch("shared.kafka_client.producer", producer_mock):
        yield producer_mock

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
        if "FROM vpa_registry" in query:
            # Provide dummy routing data for tests
            vpa = args[0] if args else "alice@hdfc"
            bank_slug = vpa.split("@")[1] if "@" in vpa else "hdfc"
            url = f"http://127.0.0.1:8001" if bank_slug == "hdfc" else "http://127.0.0.1:8002"
            return {"bank_service_url": url, "is_active": True}
        return {"balance": Decimal("1000.0")}
    
    mock_conn.fetchrow = AsyncMock(side_effect=smart_fetchrow)
    mock_conn.execute = AsyncMock(return_value="UPDATE 1")
    
    with patch("asyncpg.create_pool", return_value=mock_pool):
        yield mock_pool
