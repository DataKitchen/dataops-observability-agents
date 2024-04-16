import logging
from enum import Enum

from toolkit.observability import Status

LOGGER = logging.getLogger(__name__)


class ActivityType(Enum):
    """
    Synapse activity types

    The enum values are the same values returned by the Synapse API.
    """

    COPY = "Copy"
    EXECUTE_DATA_FLOW = "ExecuteDataFlow"
    SYNAPSE_NOTEBOOK = "SynapseNotebook"


STATUS_MAP = {
    "InProgress": Status.RUNNING,
    "Succeeded": Status.COMPLETED,
    "Uncertain": Status.COMPLETED_WITH_WARNINGS,
    "Failed": Status.FAILED,
    # Canceled shows up in the docs but cancelled show up in the UI. Covering both
    "Cancelled": Status.FAILED,
    "Canceled": Status.FAILED,
    "Queued": Status.UNKNOWN,
    "Cancelling": Status.UNKNOWN,
    "Canceling": Status.UNKNOWN,
}


def get_observability_status(status: str | None) -> Status:
    if status is None:
        LOGGER.warning("Synapse status is not set: %s", status)
        return Status.UNKNOWN
    try:
        return STATUS_MAP[status]
    except KeyError:
        LOGGER.warning("Unrecognized Synapse status: %s", status)
        return Status.UNKNOWN
