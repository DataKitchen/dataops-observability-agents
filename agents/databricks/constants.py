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


COMPONENT_TOOL: str = "databricks"
"""The Databricks tool type identifier string for Events API"""

DATABRICKS_SPN_SCOPE: str = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default"
"""The Azure Service Principal Scope assigned to Databricks"""
