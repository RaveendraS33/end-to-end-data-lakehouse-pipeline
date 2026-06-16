from src.quality.rules import (
    ERROR_INVALID_AMOUNT,
    ERROR_INVALID_EMAIL,
    ERROR_MISSING_EVENT_TIME,
    ERROR_MISSING_TRANSACTION_ID,
    ERROR_MISSING_USER_ID,
    VALID_REASON,
    validate_transaction,
)


def valid_record():
    return {
        "transaction_id": "txn-1",
        "user_id": 1001,
        "email": "student@example.com",
        "amount": 25.0,
        "event_time": "2026-06-16T00:00:00+00:00",
    }


def test_valid_transaction():
    assert validate_transaction(valid_record()) == VALID_REASON


def test_missing_transaction_id():
    record = valid_record()
    record["transaction_id"] = None
    assert validate_transaction(record) == ERROR_MISSING_TRANSACTION_ID


def test_missing_user_id():
    record = valid_record()
    record["user_id"] = None
    assert validate_transaction(record) == ERROR_MISSING_USER_ID


def test_invalid_email():
    record = valid_record()
    record["email"] = "invalid"
    assert validate_transaction(record) == ERROR_INVALID_EMAIL


def test_invalid_amount():
    record = valid_record()
    record["amount"] = 0
    assert validate_transaction(record) == ERROR_INVALID_AMOUNT


def test_missing_event_time():
    record = valid_record()
    record["event_time"] = None
    assert validate_transaction(record) == ERROR_MISSING_EVENT_TIME
