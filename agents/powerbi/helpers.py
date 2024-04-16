import logging

from trio import MemorySendChannel

from toolkit.more_typing import JSON_DICT
from toolkit.observability import EVENT_TYPE_KEY, EventType, Status

from .constants import COMPONENT_TOOL

LOGGER = logging.getLogger(__name__)


def get_status(status: str) -> Status:
    if status == "Unknown":
        return Status.RUNNING
    elif status == "Completed":
        return Status.COMPLETED
    elif status == "Cancelled":
        return Status.COMPLETED_WITH_WARNINGS
    elif status == "Failed":
        return Status.FAILED
    elif status == "Disabled":
        return Status.UNKNOWN
    else:
        LOGGER.error(f"Unrecognized status: {status}. Setting status to {Status.UNKNOWN.name}")
        return Status.UNKNOWN


async def send_run_status_event(outbound_channel: MemorySendChannel, status: Status, event_data: dict) -> None:
    event: JSON_DICT = {
        EVENT_TYPE_KEY: EventType.RUN_STATUS.value,
        "status": status.value,
        "component_tool": COMPONENT_TOOL,
        **event_data,
    }
    await outbound_channel.send(event)


async def send_message_log_event(
    outbound_channel: MemorySendChannel,
    log_level: str,
    message: str,
    event_data: dict,
) -> None:
    event: JSON_DICT = {
        EVENT_TYPE_KEY: EventType.MESSAGE_LOG.value,
        "log_level": log_level,
        "message": message,
        "component_tool": COMPONENT_TOOL,
        **event_data,
    }
    await outbound_channel.send(event)


async def send_dataset_operation_event(outbound_channel: MemorySendChannel, operation: str, event_data: dict) -> None:
    event: JSON_DICT = {
        EVENT_TYPE_KEY: EventType.DATASET_OPERATION.value,
        "operation": operation,
        "component_tool": COMPONENT_TOOL,
        **event_data,
    }
    await outbound_channel.send(event)
