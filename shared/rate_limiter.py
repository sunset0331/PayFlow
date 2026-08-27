from fastapi import HTTPException
import time
import logging

logger = logging.getLogger("payflow.rate_limiter")

# ---------------------------------------------------------------------------
# Redis failure policy for rate limiting: FAIL OPEN
#
# Rationale:
#   Rate limiting is a traffic-management mechanism, not a financial-safety
#   mechanism. If Redis is unavailable, the correct behaviour is to LOG the
#   failure prominently and allow the payment to proceed rather than blocking
#   all transactions during a Redis outage.
#
#   Contrast with idempotency (gateway/main.py): that path FAILS CLOSED (503)
#   because processing a payment without idempotency protection could cause
#   duplicate charges — a financial correctness violation.
#
#   Rate limiting failure only means an attacker could burst more requests
#   than intended during the Redis outage window. This is an acceptable
#   availability/security tradeoff.
# ---------------------------------------------------------------------------

RATE_LIMIT_REDIS_FAILURE_OPEN = True  # set to False to fail closed instead


async def enforce_rate_limits(sender_vpa: str, receiver_vpa: str, amount: float, redis_client):
    """
    Enforces 3 rules:
    1. Max 10 transactions per minute per sender (Sliding Window)
    2. Max 5 transactions to the SAME receiver per hour (Sliding Window)
    3. Max ₹100,000 transferred per day per sender (Atomic Counter)

    Redis failure policy: FAIL OPEN (logged as WARNING)
    If Redis is unavailable, limits are bypassed and a warning is emitted.
    Change RATE_LIMIT_REDIS_FAILURE_OPEN to False to block on Redis failure.
    """
    try:
        await _check_and_record(sender_vpa, receiver_vpa, amount, redis_client)
    except HTTPException:
        # Re-raise HTTP exceptions (429s) — these are intentional limit hits
        raise
    except Exception as e:
        if RATE_LIMIT_REDIS_FAILURE_OPEN:
            # Fail open: log but allow the request through
            logger.warning(
                "Rate limiter Redis unavailable — rate limit bypassed for sender=%s. Error: %s",
                sender_vpa, e,
            )
        else:
            # Fail closed: block the request
            raise HTTPException(
                status_code=503,
                detail="Rate limiting service unavailable. Please retry later."
            ) from e


RATE_LIMIT_LUA_SCRIPT = """
local min_key = KEYS[1]
local hr_key = KEYS[2]
local day_key = KEYS[3]

local now = tonumber(ARGV[1])
local amount = tonumber(ARGV[2])
local min_limit = tonumber(ARGV[3])
local hr_limit = tonumber(ARGV[4])
local day_limit = tonumber(ARGV[5])

-- Rule 3: Daily Limit
local current_daily = tonumber(redis.call('GET', day_key) or '0')
if current_daily + amount > day_limit then
    return 'DAILY_LIMIT_EXCEEDED'
end

-- Rules 1 & 2: Sliding Windows
-- Remove old timestamps
redis.call('ZREMRANGEBYSCORE', min_key, 0, now - 60)
redis.call('ZREMRANGEBYSCORE', hr_key, 0, now - 3600)

local min_count = tonumber(redis.call('ZCARD', min_key))
local hr_count = tonumber(redis.call('ZCARD', hr_key))

if min_count >= min_limit then
    return 'MIN_LIMIT_EXCEEDED'
end

if hr_count >= hr_limit then
    return 'HR_LIMIT_EXCEEDED'
end

-- All checks passed, record the new transaction
redis.call('ZADD', min_key, now, tostring(now))
redis.call('EXPIRE', min_key, 60)

redis.call('ZADD', hr_key, now, tostring(now))
redis.call('EXPIRE', hr_key, 3600)

redis.call('INCRBYFLOAT', day_key, amount)
redis.call('EXPIRE', day_key, 86400)

return 'OK'
"""

async def _check_and_record(sender_vpa: str, receiver_vpa: str, amount: float, redis_client):
    """Inner implementation — atomic evaluation via Lua script."""
    now = time.time()

    # Define Redis keys
    min_key = f"rate:min:{sender_vpa}"
    hr_key = f"rate:hr:{sender_vpa}:{receiver_vpa}"
    day_str = time.strftime("%Y-%m-%d")
    day_key = f"rate:daily:{sender_vpa}:{day_str}"

    result = await redis_client.eval(
        RATE_LIMIT_LUA_SCRIPT,
        3,  # Number of keys
        min_key, hr_key, day_key,  # KEYS
        now, amount, 10, 5, 100000  # ARGV
    )

    result_str = result.decode() if isinstance(result, bytes) else result

    if result_str == 'DAILY_LIMIT_EXCEEDED':
        raise HTTPException(status_code=429, detail="Daily limit exceeded: Max ₹100,000 per day.")
    elif result_str == 'MIN_LIMIT_EXCEEDED':
        raise HTTPException(status_code=429, detail="Rate limit exceeded: Max 10 transactions per minute.")
    elif result_str == 'HR_LIMIT_EXCEEDED':
        raise HTTPException(status_code=429, detail="Rate limit exceeded: Max 5 transactions to the same VPA per hour.")
    elif result_str == 'OK':
        return
    else:
        raise RuntimeError(f"Unexpected rate limit script result: {result_str}")