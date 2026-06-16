VALID_REASON = "valid"

ERROR_MISSING_TRANSACTION_ID = "missing_transaction_id"
ERROR_MISSING_USER_ID = "missing_user_id"
ERROR_INVALID_EMAIL = "invalid_email"
ERROR_INVALID_AMOUNT = "invalid_amount"
ERROR_MISSING_EVENT_TIME = "missing_event_time"


def validate_transaction(record):
    """Return the first data quality error reason for a transaction record."""
    if record.get("transaction_id") is None:
        return ERROR_MISSING_TRANSACTION_ID

    if record.get("user_id") is None:
        return ERROR_MISSING_USER_ID

    email = record.get("email")
    if email is None or "@" not in email:
        return ERROR_INVALID_EMAIL

    amount = record.get("amount")
    if amount is None or amount <= 0:
        return ERROR_INVALID_AMOUNT

    if record.get("event_time") is None:
        return ERROR_MISSING_EVENT_TIME

    return VALID_REASON
