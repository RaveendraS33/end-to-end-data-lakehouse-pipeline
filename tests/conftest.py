"""Pytest configuration shared across the suite.

Integration tests are tagged with the `integration` marker and are skipped
unless RUN_INTEGRATION_TESTS=1, so the fast unit suite stays runnable without
the Docker stack.
"""
import os

import pytest


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "integration: end-to-end test that requires the running Docker stack",
    )


def pytest_collection_modifyitems(config, items):
    if os.getenv("RUN_INTEGRATION_TESTS") == "1":
        return
    skip_integration = pytest.mark.skip(
        reason="integration test: start the stack and set RUN_INTEGRATION_TESTS=1 to run",
    )
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip_integration)
