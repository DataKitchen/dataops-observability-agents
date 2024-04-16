import logging

from toolkit.observability import Status

LOGGER = logging.getLogger(__name__)


def get_status(status: str) -> Status:
    status = status.upper()
    if status == "RELOADING":
        return Status.RUNNING
    elif status == "SUCCEEDED":
        return Status.COMPLETED
    elif status == "FAILED":
        return Status.FAILED
    elif status == "CANCELED":
        return Status.FAILED
    elif status == "EXCEEDED_LIMIT":
        return Status.FAILED
    else:
        LOGGER.error(f"Unrecognized status: {status}. Setting status to {Status.UNKNOWN.name}")
        return Status.UNKNOWN
