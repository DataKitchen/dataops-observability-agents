import logging

from toolkit.observability import Status

from .constants import PENDING_STATES, VALID_FAILURE_STATES

LOGGER = logging.getLogger(__name__)


def get_status(record_state: dict) -> Status:
    life_cycle_state = record_state["life_cycle_state"]
    result_state = record_state.get("result_state")

    match (life_cycle_state, result_state):
        case (lcs, _) if lcs in PENDING_STATES:
            state = Status.RUNNING
        case ("TERMINATED", "SUCCESS"):
            state = Status.COMPLETED
        case ("TERMINATED", rs) if rs in VALID_FAILURE_STATES:
            state = Status.FAILED
        case (lcs, "SKIPPED") if lcs in ("TERMINATED", "INTERNAL_ERROR"):
            state = Status.COMPLETED_WITH_WARNINGS
        case (lcs, _) if lcs in ("TERMINATED", "INTERNAL_ERROR"):
            state = Status.FAILED
        case _:
            LOGGER.error("Unrecognized status: '%s'. Setting status to '%s'", record_state, Status.UNKNOWN.name)
            state = Status.UNKNOWN

    return state


def is_a_repair_run(run_json: dict) -> bool:
    return run_json.get("trigger") == "RETRY"
