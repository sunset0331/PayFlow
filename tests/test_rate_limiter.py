import pytest
from fastapi import HTTPException
from unittest.mock import AsyncMock

from shared.rate_limiter import enforce_rate_limits

@pytest.mark.asyncio
async def test_rate_limiter_success():
    redis_mock = AsyncMock()
    redis_mock.eval.return_value = b'OK'
    
    # Should not raise any exception
    await enforce_rate_limits("alice@hdfc", "bob@sbi", 100.0, redis_mock)
    redis_mock.eval.assert_called_once()

@pytest.mark.asyncio
async def test_rate_limiter_min_exceeded():
    redis_mock = AsyncMock()
    redis_mock.eval.return_value = b'MIN_LIMIT_EXCEEDED'
    
    with pytest.raises(HTTPException) as exc_info:
        await enforce_rate_limits("alice@hdfc", "bob@sbi", 100.0, redis_mock)
    
    assert exc_info.value.status_code == 429
    assert "minute" in exc_info.value.detail

@pytest.mark.asyncio
async def test_rate_limiter_hr_exceeded():
    redis_mock = AsyncMock()
    redis_mock.eval.return_value = b'HR_LIMIT_EXCEEDED'
    
    with pytest.raises(HTTPException) as exc_info:
        await enforce_rate_limits("alice@hdfc", "bob@sbi", 100.0, redis_mock)
    
    assert exc_info.value.status_code == 429
    assert "hour" in exc_info.value.detail

@pytest.mark.asyncio
async def test_rate_limiter_daily_exceeded():
    redis_mock = AsyncMock()
    redis_mock.eval.return_value = b'DAILY_LIMIT_EXCEEDED'
    
    with pytest.raises(HTTPException) as exc_info:
        await enforce_rate_limits("alice@hdfc", "bob@sbi", 100.0, redis_mock)
    
    assert exc_info.value.status_code == 429
    assert "Max ₹100,000" in exc_info.value.detail

@pytest.mark.asyncio
async def test_rate_limiter_redis_failure_fail_open():
    redis_mock = AsyncMock()
    redis_mock.eval.side_effect = Exception("Redis connection refused")
    
    # Due to RATE_LIMIT_REDIS_FAILURE_OPEN = True, this should NOT raise an exception
    # It just logs a warning and allows the payment to proceed.
    await enforce_rate_limits("alice@hdfc", "bob@sbi", 100.0, redis_mock)
