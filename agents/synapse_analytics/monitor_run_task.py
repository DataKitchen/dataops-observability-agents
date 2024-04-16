import logging
from datetime import datetime
from typing import cast

from azure.core.exceptions import ResourceNotFoundError
from azure.synapse.artifacts.models import (
    PipelineRun,
    PipelineRunInvokedBy,
)
from trio import MemorySendChannel

from framework.core.tasks.periodic_task import PeriodicTask
from toolkit.more_typing import JSON_DICT
from toolkit.observability import EVENT_TYPE_KEY, EventType, Status

from .click_back_urls import pipeline_click_back_url
from .client import artifacts_client
from .constants import COMPONENT_TOOL
from .helpers import get_observability_status
from .monitor_activities import MonitorActivities
from .types import SynapseRunData

LOGGER = logging.getLogger(__name__)


class MonitorRunTask(PeriodicTask):
    synapse_run: SynapseRunData

    def __init__(
        self,
        synapse_run: SynapseRunData,
        initial_start_time: datetime,
        outbound_channel: MemorySendChannel[JSON_DICT],
    ) -> None:
        super().__init__(outbound_channel=outbound_channel)
        self.synapse_run = synapse_run
        self.finish_counter = 0
        self.status = Status.UNKNOWN
        self.click_back_url = pipeline_click_back_url(synapse_run.run_id)
        self.monitor_activities = MonitorActivities(
            synapse_run=synapse_run,
            initial_start_time=initial_start_time,
            # Mypy bug; see https://github.com/python/mypy/issues/16659
            outbound_channel=cast(MemorySendChannel[JSON_DICT], self.outbound_channel),
        )

    async def _send_run_status(self, status: Status, event_timestamp: datetime | None, metadata: dict | None) -> None:
        payload: JSON_DICT = {
            EVENT_TYPE_KEY: EventType.RUN_STATUS.value,
            "status": status.value,
            "metadata": metadata,
            "event_timestamp": event_timestamp.astimezone().isoformat() if event_timestamp else None,
            "pipeline_key": self.synapse_run.pipeline_name,
            "run_key": self.synapse_run.run_id,
            "external_url": self.click_back_url,
        }
        if status is Status.RUNNING:
            payload.update({"component_tool": COMPONENT_TOOL, "pipeline_name": self.synapse_run.pipeline_name})
        await self.send(payload)

    async def _execute(self, current_dt: datetime, previous_dt: datetime) -> None:
        run: PipelineRun = await artifacts_client().pipeline_run.get_pipeline_run(run_id=self.synapse_run.run_id)
        prev_status = self.status
        if (status := get_observability_status(run.status)) is not Status.UNKNOWN:
            self.status = status

        if prev_status is Status.UNKNOWN and self.status is not Status.UNKNOWN:
            LOGGER.debug("Run %s started", run.run_id)
            metadata = {
                "parameters": run.parameters,
                "invoked_by": cast(PipelineRunInvokedBy, run.invoked_by).as_dict() if run.invoked_by else None,
            }
            await self._send_run_status(Status.RUNNING, run.run_start, metadata)

        await self.monitor_activities.update(current_dt, previous_dt)

        # Finish the run on the second "finished" loop because the run status is always current whereas the activities
        # are time ranged. All activity info may not be within the time range as the run might have ended after it was set.
        self.finish_counter += 1 if status.finished else 0
        if self.finish_counter >= 2:
            LOGGER.info("Run %s finished with status %s", run.run_id, status.value)
            metadata = {
                "run_duration_ms": run.duration_in_ms,
            }
            await self._send_run_status(status, run.run_end, metadata)
            self.finish()

    async def execute(self, current_dt: datetime, previous_dt: datetime) -> None:
        try:
            await self._execute(current_dt, previous_dt)
        except ResourceNotFoundError:
            LOGGER.exception("Run '%s' not found", self.synapse_run.pipeline_name)
            self.finish()
