from enum import Enum

EVENT_TYPE_KEY = "EVENT_TYPE"


class EventType(Enum):
    """
    This Enum class maps to the url paths of event types accepted by the `Events Ingestion API
    """

    DATASET_OPERATION = "dataset-operation"
    MESSAGE_LOG = "message-log"
    METRIC_LOG = "metric-log"
    RUN_STATUS = "run-status"
    TEST_OUTCOMES = "test-outcomes"
