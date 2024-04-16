from enum import Enum

EVENT_TYPE_KEY: str = "EVENT_TYPE"
"""
The expected key-map for event-type in a payload.
"""


class EventType(Enum):
    """
    This Enum class maps to the url paths of event types accepted by the `Events Ingestion API`
    See: https://api.docs.datakitchen.io/
    """

    DATASET_OPERATION = "dataset-operation"
    MESSAGE_LOG = "message-log"
    METRIC_LOG = "metric-log"
    RUN_STATUS = "run-status"
    TEST_OUTCOMES = "test-outcomes"
