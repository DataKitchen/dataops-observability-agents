VALID_FAILURE_STATES: frozenset[str] = frozenset(
    (
        "FAILED",
        "TIMEDOUT",
        "CANCELED",
        "MAXIMUM_CONCURRENT_RUNS_REACHED",
        "EXCLUDED",
        "SUCCESS_WITH_FAILURES",
        "UPSTREAM_FAILED",
        "UPSTREAM_CANCELED",
    ),
)
"""Valid Databricks Failure states"""

PENDING_STATES: frozenset[str] = frozenset(("PENDING", "RUNNING", "TERMINATING", "WAITING_FOR_RETRY", "BLOCKED"))
"""Databricks pending states"""


COMPONENT_TOOL: str = "qlik"
"""The Qlik tool type identifier string for Events API"""
