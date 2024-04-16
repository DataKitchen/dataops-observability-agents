import logging

from toolkit.observability import Status

LOG = logging.getLogger(__name__)


def get_status(status_str: str) -> Status:
    # Airflow Dag Run States: "queued", "running", "success", "failed".
    match status_str.lower():
        case "queued":
            return Status.UNKNOWN
        case "running":
            return Status.RUNNING
        case "success":
            return Status.COMPLETED
        case "failed":
            return Status.FAILED
        case _:
            LOG.warning("Unrecognized status: %s; defaulting to %s", status_str, Status.UNKNOWN.name)
            return Status.UNKNOWN
