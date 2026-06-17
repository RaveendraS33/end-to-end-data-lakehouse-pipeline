from src.quality.rules import (
    ERROR_INVALID_AMOUNT,
    ERROR_INVALID_CURRENCY,
    ERROR_INVALID_EMAIL,
    ERROR_INVALID_EVENT_TIME,
    ERROR_INVALID_STATUS,
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
        "currency": "USD",
        "status": "SUCCESS",
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


def test_invalid_email_without_at():
    record = valid_record()
    record["email"] = "invalid"
    assert validate_transaction(record) == ERROR_INVALID_EMAIL


def test_invalid_email_without_domain_dot():
    record = valid_record()
    record["email"] = "user@localhost"
    assert validate_transaction(record) == ERROR_INVALID_EMAIL


def test_invalid_amount():
    record = valid_record()
    record["amount"] = 0
    assert validate_transaction(record) == ERROR_INVALID_AMOUNT


def test_missing_event_time():
    record = valid_record()
    record["event_time"] = None
    assert validate_transaction(record) == ERROR_MISSING_EVENT_TIME


def test_unparseable_event_time():
    record = valid_record()
    record["event_time"] = "not-a-timestamp"
    assert validate_transaction(record) == ERROR_INVALID_EVENT_TIME


def test_event_time_with_z_suffix_is_valid():
    record = valid_record()
    record["event_time"] = "2026-06-16T00:00:00Z"
    assert validate_transaction(record) == VALID_REASON


def test_invalid_currency():
    record = valid_record()
    record["currency"] = "BTC"
    assert validate_transaction(record) == ERROR_INVALID_CURRENCY


def test_invalid_status():
    record = valid_record()
    record["status"] = "UNKNOWN"
    assert validate_transaction(record) == ERROR_INVALID_STATUS
