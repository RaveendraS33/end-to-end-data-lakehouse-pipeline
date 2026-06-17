"""Single source of truth for transaction data quality rules.

The pure-Python `validate_transaction` is used by unit tests and documents the
intent rule-by-rule. The Spark job (`spark_jobs/lakehouse_common.py`) imports
the shared constants/patterns below and expresses the *same* rules as column
logic, so the two implementations cannot drift on what counts as valid.
"""
import re
from datetime import datetime

VALID_REASON = "valid"

ERROR_MISSING_TRANSACTION_ID = "missing_transaction_id"
ERROR_MISSING_USER_ID = "missing_user_id"
ERROR_INVALID_EMAIL = "invalid_email"
ERROR_INVALID_AMOUNT = "invalid_amount"
ERROR_MISSING_EVENT_TIME = "missing_event_time"
ERROR_INVALID_EVENT_TIME = "invalid_event_time"
ERROR_INVALID_CURRENCY = "invalid_currency"
ERROR_INVALID_STATUS = "invalid_status"

# Java- and Python-compatible: a single @, no whitespace, and a dotted domain.
EMAIL_REGEX = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
_EMAIL_PATTERN = re.compile(EMAIL_REGEX)

ALLOWED_CURRENCIES = ("USD", "EUR", "GBP")
ALLOWED_STATUSES = ("SUCCESS", "FAILED", "PENDING")


def _is_parseable_timestamp(value: str) -> bool:
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
        return True
    except (ValueError, AttributeError):
        return False


def validate_transaction(record):
    """Return the first data quality error reason for a transaction record.

    Rules are ordered: a record fails on the first rule it violates.
    """
    if record.get("transaction_id") is None:
        return ERROR_MISSING_TRANSACTION_ID

    if record.get("user_id") is None:
        return ERROR_MISSING_USER_ID

    email = record.get("email")
    if email is None or not _EMAIL_PATTERN.match(email):
        return ERROR_INVALID_EMAIL

    amount = record.get("amount")
    if amount is None or amount <= 0:
        return ERROR_INVALID_AMOUNT

    event_time = record.get("event_time")
    if event_time is None:
        return ERROR_MISSING_EVENT_TIME
    if not _is_parseable_timestamp(event_time):
        return ERROR_INVALID_EVENT_TIME

    if record.get("currency") not in ALLOWED_CURRENCIES:
        return ERROR_INVALID_CURRENCY

    if record.get("status") not in ALLOWED_STATUSES:
        return ERROR_INVALID_STATUS

    return VALID_REASON
