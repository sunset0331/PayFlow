import pytest
import asyncio
from unittest.mock import AsyncMock, patch, MagicMock
from services.payment_worker.main import process_command

@pytest.mark.asyncio
async def test_payment_worker_success():
    producer_mock = AsyncMock()
    event = {
        "txn_id": "test-txn",
        "event_type": "debit_request",
        "payload": {
            "vpa": "test@hdfc",
            "amount": 100.0,
            "bank_url": "http://bank-hdfc:8001"
        }
    }
    
    with patch("httpx.AsyncClient.post") as mock_post:
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_post.return_value = mock_response
        
        await process_command(event, producer_mock)
        
        # Verify bank call
        mock_post.assert_called_once()
        
        # Verify event published
        producer_mock.send_and_wait.assert_called_once()

@pytest.mark.asyncio
async def test_payment_worker_failure_raises():
    producer_mock = AsyncMock()
    # Malformed event that will cause an exception
    event = {
        "txn_id": "test-txn",
        "event_type": "debit_request",
        # Missing payload fields
    }
    
    # process_command won't throw for missing payload, it just returns. 
    # Let's test the consumer loop behavior indirectly or test process_command throws on network failure if we mocked publisher.
    
    # Since process_command catches httpx exceptions and publishes ambiguous, the only unhandled exceptions are
    # if publish itself fails.
    event["payload"] = {"vpa": "t@h", "amount": 10, "bank_url": "http://foo"}
    
    with patch("httpx.AsyncClient.post") as mock_post:
        mock_response = MagicMock()
        mock_post.return_value = mock_response
        
        # Make the producer fail
        producer_mock.send_and_wait.side_effect = Exception("Kafka down")
        
        with pytest.raises(Exception, match="Kafka down"):
            await process_command(event, producer_mock)
