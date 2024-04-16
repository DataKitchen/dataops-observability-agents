from enum import Enum


class Status(Enum):
    """
    This Enum class maps to the status types accepted by the `Events Ingestion API
    <https://api.docs.datakitchen.io/production/events.html>`_ - except for UNKNOWN. The
    UNKNOWN status is a catchall for those that don't map. It is a Plugin's responsibility
    to handle this status accordingly, typically by ignoring events with an UNKNOWN status.
    """

    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    COMPLETED_WITH_WARNINGS = "COMPLETED_WITH_WARNINGS"
    FAILED = "FAILED"
    UNKNOWN = "UNKNOWN"

    @property
    def finished(self) -> bool:
        """
        Return True if the status represents a finished state, False otherwise.

        Returns
        -------
        bool
            True if the status represents a finished state, False otherwise.
        """
        return self in [Status.COMPLETED, Status.COMPLETED_WITH_WARNINGS, Status.FAILED]
