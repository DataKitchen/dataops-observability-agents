from enum import Enum


class MessageEventLogLevel(Enum):
    """
    This Enum class maps to the log levels accepted by the `Events Ingestion API
    <https://api.docs.datakitchen.io/production/events.html>`.
    """

    ERROR = "ERROR"
    INFO = "INFO"
    WARNING = "WARNING"
