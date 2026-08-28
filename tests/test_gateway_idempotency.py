import pytest
from httpx import AsyncClient
import respx
from httpx import Response

@pytest.mark.asyncio
async def test_gateway_idempotency_duplicate_payload(gateway_client: AsyncClient, mock_redis):
    payload = {
        "sender_vpa": "alice@hdfc",
        "receiver_vpa": "bob@sbi",
        "amount": 100.0
    }
    
    with respx.mock(assert_all_called=False) as respx_mock:
        # Mock bank calls to succeed
        respx_mock.post("http://127.0.0.1:8001/debit").mock(return_value=Response(200, json={"status": "success"}))
        idemp_key = "test-key"
        
        # Request 1 (should succeed and start saga)
        response1 = await gateway_client.post(
            "/pay", 
            json=payload,
            headers={"Idempotency-Key": idemp_key}
        )
        assert response1.status_code == 202
        data1 = response1.json()
        assert data1["status"] == "PROCESSING"
        assert "txn_id" in data1
        
        # Idempotency is handled via locking and we get back exactly what redis holds?
        # Wait, the endpoint returns:
        # return {
        #     "status": "PROCESSING",
        #     "txn_id": txn_id,
        #     "message": "Payment initiated and is processing in the background."
        # }
        # And it DOES NOT cache 202s in Redis to allow the worker to cache the final SUCCESS/FAILURE.
        # But wait, idempotency middleware locks the request during processing and returns HTTP 409 or something?
        # Actually, let's just make the test assert we get a 409 Conflict if we retry immediately (or 202 if it's considered valid).
        
        # In our implementation:
        # idempotency middleware uses redis SET NX.
        # If the lock is held (PROCESSING), it returns 409 Conflict: "A request with this Idempotency-Key is currently being processed."
        idemp_key = "test-key"
        
        # Request 2 (concurrent duplicate BEFORE processing finishes)
        response2 = await gateway_client.post(
            "/pay", 
            json=payload,
            headers={"Idempotency-Key": idemp_key} # Use a fixed key
        )
        assert response2.status_code == 409
