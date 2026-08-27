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


async def _check_and_record(sender_vpa: str, receiver_vpa: str, amount: float, redis_client):
    """Inner implementation — all Redis calls are here so the outer function can catch cleanly."""
    now = time.time()

    # Define Redis keys
    min_key = f"rate:min:{sender_vpa}"
    hr_key = f"rate:hr:{sender_vpa}:{receiver_vpa}"

    # We use the current date string for the daily limit
    day_str = time.strftime("%Y-%m-%d")
    day_key = f"rate:daily:{sender_vpa}:{day_str}"

    # RULE 3: Check Daily Limit First (simplest)
    current_daily = await redis_client.get(day_key)
    current_daily = float(current_daily) if current_daily else 0.0
    if current_daily + amount > 100000:
        raise HTTPException(status_code=429, detail="Daily limit exceeded: Max ₹100,000 per day.")

    # RULE 1 & 2: Sliding Windows
    # Remove timestamps older than the window
    await redis_client.zremrangebyscore(min_key, 0, now - 60)
    await redis_client.zremrangebyscore(hr_key, 0, now - 3600)

    # Count how many requests occurred inside the remaining window
    min_count = await redis_client.zcard(min_key)
    hr_count = await redis_client.zcard(hr_key)

    if min_count >= 10:
        raise HTTPException(status_code=429, detail="Rate limit exceeded: Max 10 transactions per minute.")

    if hr_count >= 5:
        raise HTTPException(status_code=429, detail="Rate limit exceeded: Max 5 transactions to the same VPA per hour.")

    # ALL CHECKS PASSED -> Record this new transaction in Redis
    # We use a Pipeline to execute all Redis commands in a single network trip for ultra-low latency
    pipe = redis_client.pipeline()

    # Add current timestamp to sorted sets
    pipe.zadd(min_key, {str(now): now})
    pipe.expire(min_key, 60)  # Cleanup memory after window passes

    pipe.zadd(hr_key, {str(now): now})
    pipe.expire(hr_key, 3600)

    # Increment daily amount tracker
    pipe.incrbyfloat(day_key, amount)
    pipe.expire(day_key, 86400)  # Expire after 24 hours

    await pipe.execute()