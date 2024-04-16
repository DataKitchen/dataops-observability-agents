import logging
from abc import ABCMeta, abstractmethod
from datetime import datetime

from trio import MemorySendChannel

from toolkit.more_typing import JSON_DICT
from toolkit.observability import EVENT_TYPE_KEY, EventType, Status

from .click_back_urls import activity_click_back_url
from .helpers import ActivityType, get_observability_status
from .types import SynapseActivityData

LOGGER = logging.getLogger(__name__)


class SynapseActivityStateBase(metaclass=ABCMeta):
    """
    Keeps the state of a Synapse activity

    An activity is the Synapse equivalent of an Observability task.
    """

    activity_data: SynapseActivityData
    status: Status

    def __init__(
        self,
        activity_data: SynapseActivityData,
        outbound_channel: MemorySendChannel[JSON_DICT],
    ) -> None:
        self.activity_data = activity_data
        self.status = Status.UNKNOWN
        self.send_to = outbound_channel
        self.click_back_url = activity_click_back_url(self.activity_data, self.activity_data.pipeline_run_id)

    @property
    def finished(self) -> bool:
        return self.status.finished

    @abstractmethod
    async def type_update(self, activity_data: SynapseActivityData) -> None:
        ...

    async def _send_task_status(self, status: Status, event_timestamp: datetime | None, metadata: dict | None) -> None:
        payload: JSON_DICT = {
            EVENT_TYPE_KEY: EventType.RUN_STATUS.value,
            "status": status.value,
            "metadata": metadata,
            "event_timestamp": event_timestamp.astimezone().isoformat() if event_timestamp else None,
            "pipeline_key": self.activity_data.pipeline_name,
            "run_key": self.activity_data.pipeline_run_id,
            "task_key": self.activity_data.activity_name,
            "external_url": self.click_back_url,
        }
        if status is Status.RUNNING:
            payload["task_name"] = self.activity_data.activity_name
        await self.send_to.send(payload)

    async def _send_error_message_log(
        self,
        message: str,
        event_timestamp: datetime | None,
        metadata: dict | None,
    ) -> None:
        await self.send_to.send(
            {
                EVENT_TYPE_KEY: EventType.MESSAGE_LOG.value,
                "message": message,
                # TODO: Log enum?
                "log_level": "ERROR",
                "metadata": metadata,
                "event_timestamp": event_timestamp.astimezone().isoformat() if event_timestamp else None,
                "pipeline_key": self.activity_data.pipeline_name,
                "run_key": self.activity_data.pipeline_run_id,
                "task_key": self.activity_data.activity_name,
                "external_url": self.click_back_url,
            },
        )

    async def update(self, activity_data: SynapseActivityData) -> None:
        prev_status = self.status
        if (status := get_observability_status(activity_data.status)) is not Status.UNKNOWN:
            self.status = status
        if self.status in {Status.UNKNOWN, prev_status}:
            return
        LOGGER.debug(
            "Run %s, Activity %s: %s",
            self.activity_data.pipeline_run_id,
            self.activity_data.activity_name,
            self.status.value,
        )

        metadata: JSON_DICT = {
            "activity_run_id": activity_data.activity_run_id,
            "activity_type": activity_data.activity_type,
        }
        if prev_status is Status.UNKNOWN:
            LOGGER.debug(
                "Run %s, starting activity %s",
                self.activity_data.pipeline_run_id,
                self.activity_data.activity_name,
            )
            await self._send_task_status(
                Status.RUNNING,
                activity_data.activity_run_start,
                metadata | {"activity_input": activity_data.input_},
            )

        await self.type_update(activity_data)

        if self.finished:
            LOGGER.debug(
                "Run %s, ending activity %s",
                self.activity_data.pipeline_run_id,
                self.activity_data.activity_name,
            )

            if (
                activity_data.error
                and ((message := activity_data.error.get("message")) is not None)
                and len(message) > 0
            ):
                await self._send_error_message_log(message, activity_data.activity_run_end, metadata)
            metadata["activity_output"] = activity_data.output
            await self._send_task_status(self.status, activity_data.activity_run_end, metadata)


class SynapseActivityState(SynapseActivityStateBase):
    async def type_update(self, activity_data: SynapseActivityData) -> None:
        ...


class CopyActivityState(SynapseActivityStateBase):
    # TODO: Operation enum
    async def _send_dataset_operation(self, dataset: str, operation: str, event_timestamp: datetime | None) -> None:
        metadata: JSON_DICT = {
            "pipeline_name": self.activity_data.pipeline_name,
            "pipeline_run_id": self.activity_data.pipeline_run_id,
            "activity_name": self.activity_data.activity_name,
            "activity_run_id": self.activity_data.activity_run_id,
        }
        payload: JSON_DICT = {
            EVENT_TYPE_KEY: EventType.DATASET_OPERATION.value,
            "dataset_key": dataset,
            "operation": operation,
            "metadata": metadata,
            "event_timestamp": event_timestamp.astimezone().isoformat() if event_timestamp else None,
            "external_url": self.click_back_url,
        }
        await self.send_to.send(payload)

    async def type_update(self, activity_data: SynapseActivityData) -> None:
        if self.status in {Status.COMPLETED, Status.COMPLETED_WITH_WARNINGS}:
            if activity_data.additional_properties and (
                user_props := activity_data.additional_properties.get("userProperties")
            ):
                if source := user_props.get("Source"):
                    await self._send_dataset_operation(source, "READ", activity_data.activity_run_end)
                if destination := user_props.get("Destination"):
                    await self._send_dataset_operation(destination, "WRITE", activity_data.activity_run_end)


ACTIVITY_TYPES = {
    ActivityType.COPY.value: CopyActivityState,
}


def create_activity_state(
    activity_data: SynapseActivityData,
    outbound_channel: MemorySendChannel[JSON_DICT],
) -> SynapseActivityStateBase:
    if (state_type := ACTIVITY_TYPES.get(activity_data.activity_type)) is None:
        return SynapseActivityState(
            activity_data=activity_data,
            outbound_channel=outbound_channel,
        )
    return state_type(
        activity_data=activity_data,
        outbound_channel=outbound_channel,
    )
