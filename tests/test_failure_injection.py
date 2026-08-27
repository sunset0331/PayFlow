import pytest
from httpx import AsyncClient, Response, ConnectTimeout, ReadTimeout
import respx
from unittest.mock import patch

@pytest.mark.asyncio
async def test_failure_injection_bank_timeout(gateway_client: AsyncClient, mock_redis):
    """
    Test scenario where the sender bank times out on debit.
    The Gateway should fail fast and NOT proceed to credit.
    """
    payload = {
        "sender_vpa": "alice@hdfc",
        "receiver_vpa": "bob@sbi",
        "amount": 50.0
    }
    
    with respx.mock(assert_all_called=False) as respx_mock:
        # Step 1: Debit times out
        respx_mock.post("http://127.0.0.1:8001/debit").mock(side_effect=ConnectTimeout("Bank slow"))
        
        response = await gateway_client.post("/pay", json=payload)
        
        assert response.status_code == 502
        assert "Sender bank unreachable" in response.json()["detail"]

@pytest.mark.asyncio
async def test_failure_injection_bank_503(gateway_client: AsyncClient, mock_redis):
    """
    Test scenario where the sender bank returns 503 Service Unavailable.
    The Gateway should return 502 Bad Gateway to the client.
    """
    payload = {
        "sender_vpa": "alice@hdfc",
        "receiver_vpa": "bob@sbi",
        "amount": 50.0
    }
    
    with respx.mock(assert_all_called=False) as respx_mock:
        # Step 1: Debit returns 503
        respx_mock.post("http://127.0.0.1:8001/debit").mock(return_value=Response(503))
        
        response = await gateway_client.post("/pay", json=payload)
        
        assert response.status_code == 502
        assert "Sender bank unavailable" in response.json()["detail"]

@pytest.mark.asyncio
async def test_failure_injection_redis_timeout_idempotency(gateway_client: AsyncClient, mock_redis):
    """
    Test scenario where Redis is completely down (idempotency fails).
    The Gateway MUST fail closed (503) and not process the payment.
    """
    payload = {
        "sender_vpa": "alice@hdfc",
        "receiver_vpa": "bob@sbi",
        "amount": 50.0
    }
    
    with patch.object(mock_redis, "set", side_effect=Exception("Redis connection timed out")):
        response = await gateway_client.post("/pay", json=payload)
    
    assert response.status_code == 503
    assert "Idempotency service unavailable" in response.json()["detail"]
