import pytest

@pytest.mark.asyncio
async def test_gateway_payment_validation(gateway_client):
    # Test zero amount
    payload = {
        "sender_vpa": "utkarsh@hdfc",
        "receiver_vpa": "alice@sbi",
        "amount": 0.0,
        "currency": "INR"
    }
    response = await gateway_client.post("/pay", json=payload)
    assert response.status_code == 422
    assert "greater than 0" in response.text

    # Test negative amount
    payload["amount"] = -100.50
    response = await gateway_client.post("/pay", json=payload)
    assert response.status_code == 422
    assert "greater than 0" in response.text

@pytest.mark.asyncio
async def test_bank_validation(hdfc_client):
    # Test zero amount in debit
    payload = {
        "vpa": "utkarsh@hdfc",
        "amount": 0.0,
        "txn_id": "00000000-0000-0000-0000-000000000000"
    }
    response = await hdfc_client.post("/debit", json=payload)
    assert response.status_code == 422
    assert "greater than 0" in response.text

    # Test negative amount in credit
    payload["amount"] = -50.0
    response = await hdfc_client.post("/credit", json=payload)
    assert response.status_code == 422
    assert "greater than 0" in response.text
