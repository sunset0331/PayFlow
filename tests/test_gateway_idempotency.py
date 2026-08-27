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
        respx_mock.post("http://127.0.0.1:8002/credit").mock(return_value=Response(200, json={"status": "success"}))
        
        # Request 1 (should succeed and start saga)
        response1 = await gateway_client.post("/pay", json=payload)
        assert response1.status_code == 200
        data1 = response1.json()
        assert data1["status"] == "SUCCESS"
        assert "idempotency_key" in data1
        
        idemp_key = data1["idempotency_key"]
        
        # Request 2 (concurrent duplicate BEFORE cache is set)
        response2 = await gateway_client.post(
            "/pay", 
            json=payload,
            headers={"Idempotency-Key": idemp_key}
        )
        assert response2.status_code == 200
        assert response2.json() == data1
