import pytest
from httpx import AsyncClient, Response, ConnectError
import respx

@pytest.mark.asyncio
async def test_saga_compensation_on_credit_failure(gateway_client: AsyncClient, mock_redis):
    payload = {
        "sender_vpa": "alice@hdfc",
        "receiver_vpa": "bob@sbi",
        "amount": 100.0
    }
    
    with respx.mock(assert_all_called=False) as respx_mock:
        # Step 1: Debit succeeds
        respx_mock.post("http://127.0.0.1:8001/debit").mock(return_value=Response(200, json={"status": "success"}))
        
        # Step 2: Credit fails due to network error
        respx_mock.post("http://127.0.0.1:8002/credit").mock(side_effect=ConnectError("Connection refused"))
        
        # Step 3: Gateway queries receiver bank for credit status -> returns 404 (not executed)
        respx_mock.get(url__regex=r"http://127\.0\.0\.1:8002/transaction/.*").mock(return_value=Response(404))
        
        # Step 4: Compensation executes (refund to sender)
        respx_mock.post("http://127.0.0.1:8001/credit").mock(return_value=Response(200, json={"status": "success"}))
        
        response = await gateway_client.post("/pay", json=payload)
        
        # The gateway raises 500 when compensation completes successfully
        assert response.status_code == 500
        assert "Receiver bank failed. Payment reversed and sender refunded" in response.json()["detail"]

@pytest.mark.asyncio
async def test_saga_lost_response_scenario(gateway_client: AsyncClient, mock_redis):
    payload = {
        "sender_vpa": "alice@hdfc",
        "receiver_vpa": "bob@sbi",
        "amount": 100.0
    }
    
    with respx.mock(assert_all_called=False) as respx_mock:
        # Step 1: Debit succeeds
        respx_mock.post("http://127.0.0.1:8001/debit").mock(return_value=Response(200, json={"status": "success"}))
        
        # Step 2: Credit times out (but actually succeeded on the bank's side!)
        respx_mock.post("http://127.0.0.1:8002/credit").mock(side_effect=ConnectError("Connection timed out"))
        
        # Step 3: Gateway queries receiver bank for credit status -> returns 200 (executed successfully)
        respx_mock.get(url__regex=r"http://127\.0\.0\.1:8002/transaction/.*").mock(return_value=Response(200))
        
        response = await gateway_client.post("/pay", json=payload)
        
        # The gateway should treat this as a SUCCESS because the bank confirmed the credit
        assert response.status_code == 200
        assert response.json()["status"] == "SUCCESS"
