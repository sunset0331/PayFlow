import pytest
from services.reconciliation_worker.main import evaluate_reconciliation

def test_reconciliation_matched_completed():
    status, discrepancy = evaluate_reconciliation(
        saga_state="COMPLETED",
        debit_status="SUCCESS",
        credit_status="SUCCESS",
        ledger_events={"PAYMENT_SUCCESS": True}
    )
    assert status == "MATCHED"
    assert discrepancy == "NONE"

def test_reconciliation_mismatch_missing_debit():
    status, discrepancy = evaluate_reconciliation(
        saga_state="COMPLETED",
        debit_status="NOT_FOUND",
        credit_status="SUCCESS",
        ledger_events={"PAYMENT_SUCCESS": True}
    )
    assert status == "MISMATCH"
    assert discrepancy == "DEBIT_MISSING"

def test_reconciliation_mismatch_missing_credit():
    status, discrepancy = evaluate_reconciliation(
        saga_state="COMPLETED",
        debit_status="SUCCESS",
        credit_status="NOT_FOUND",
        ledger_events={"PAYMENT_SUCCESS": True}
    )
    assert status == "MISMATCH"
    assert discrepancy == "CREDIT_MISSING"

def test_reconciliation_mismatch_missing_ledger():
    status, discrepancy = evaluate_reconciliation(
        saga_state="COMPLETED",
        debit_status="SUCCESS",
        credit_status="SUCCESS",
        ledger_events={}
    )
    assert status == "MISMATCH"
    assert discrepancy == "LEDGER_MISSING"

def test_reconciliation_matched_compensated():
    status, discrepancy = evaluate_reconciliation(
        saga_state="COMPENSATED",
        debit_status="SUCCESS",
        credit_status="NOT_FOUND",
        ledger_events={"COMPENSATION_SUCCESS": True},
        comp_status="SUCCESS"
    )
    assert status == "MATCHED"
    assert discrepancy == "NONE"

def test_reconciliation_mismatch_credit_exists_for_compensated():
    # If saga was compensated, but receiver was credited, we have dual spend!
    status, discrepancy = evaluate_reconciliation(
        saga_state="COMPENSATED",
        debit_status="SUCCESS",
        credit_status="SUCCESS",
        ledger_events={"COMPENSATION_SUCCESS": True},
        comp_status="SUCCESS"
    )
    assert status == "MISMATCH"
    assert discrepancy == "CREDIT_EXISTS_FOR_COMPENSATED"

def test_reconciliation_indeterminate_bank_unavailable():
    status, discrepancy = evaluate_reconciliation(
        saga_state="COMPLETED",
        debit_status="SUCCESS",
        credit_status="UNAVAILABLE",
        ledger_events={"PAYMENT_SUCCESS": True}
    )
    assert status == "INDETERMINATE"
    assert discrepancy == "BANK_STATUS_UNAVAILABLE"

def test_reconciliation_indeterminate_saga_stuck():
    status, discrepancy = evaluate_reconciliation(
        saga_state="INDETERMINATE",
        debit_status="SUCCESS",
        credit_status="NOT_FOUND",
        ledger_events={}
    )
    assert status == "INDETERMINATE"
    assert discrepancy == "SAGA_STUCK"

def test_reconciliation_matched_failed_no_money_moved():
    status, discrepancy = evaluate_reconciliation(
        saga_state="FAILED",
        debit_status="NOT_FOUND",
        credit_status="NOT_FOUND",
        ledger_events={}
    )
    assert status == "MATCHED"
    assert discrepancy == "NONE"

def test_reconciliation_mismatch_failed_but_debited():
    status, discrepancy = evaluate_reconciliation(
        saga_state="FAILED",
        debit_status="SUCCESS",
        credit_status="NOT_FOUND",
        ledger_events={}
    )
    assert status == "MISMATCH"
    assert discrepancy == "DEBIT_EXISTS_FOR_FAILED"
