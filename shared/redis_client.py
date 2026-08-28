import redis.asyncio as redis
import os

# Connects to the Redis container via the Docker bridge network
# Fallback to docker redis internal hostname
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")

# Connection pool manages multiple connections efficiently
redis_pool = redis.ConnectionPool.from_url(REDIS_URL)

def get_redis():
    """Dependency injection for Redis client."""
    return redis.Redis(connection_pool=redis_pool)