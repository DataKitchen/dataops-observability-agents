import logging
from collections.abc import AsyncGenerator
from datetime import datetime
from typing import cast

from azure.synapse.artifacts.models import (
    RunFilterParameters,
    RunQueryFilter,
    RunQueryFilterOperand,
    RunQueryFilterOperator,
)
from trio import MemorySendChannel, Nursery

from framework.core.loops.periodic_loop import PeriodicLoop
from framework.core.tasks.periodic_task import PeriodicTask
from registry import ConfigurationRegistry
from toolkit.more_typing import JSON_DICT

from .client import artifacts_client
from .config import SynapseAnalyticsConfiguration
from .monitor_run_task import MonitorRunTask
from .types import SynapseRunData

LOGGER = logging.getLogger(__name__)


# No particular API error handling is done here as the azure lib handles rate limiting and other types of errors are
# caught by the task caller.
class ListRunsTask(PeriodicTask):
    nursery: Nursery
    watched_runs: dict[str, MonitorRunTask]

    def __init__(
        self,
        nursery: Nursery,
        outbound_channel: MemorySendChannel[JSON_DICT],
    ):
        super().__init__(outbound_channel=outbound_channel)
        self.configuration = ConfigurationRegistry().lookup("synapse_analytics", SynapseAnalyticsConfiguration)
        self.nursery = nursery
        self.watched_runs = {}
        LOGGER.debug(
            "Observing Synapse Analytics for the following pipelines: %s",
            self.configuration.pipelines_filter,
        )

    async def execute(self, current_dt: datetime, previous_dt: datetime) -> None:
        LOGGER.debug("Requesting Synapse run list")
        async for run in self._get_new_synapse_runs(current_dt, previous_dt):
            LOGGER.debug("Found new run, pipeline: %s, run id: %s", run.pipeline_name, run.run_id)

            monitor_run = MonitorRunTask(
                synapse_run=run,
                initial_start_time=previous_dt,
                # cast handles a mypy bug; see https://github.com/python/mypy/issues/16659
                outbound_channel=cast(MemorySendChannel[JSON_DICT], self.outbound_channel.clone()),
            )
            self.nursery.start_soon(
                PeriodicLoop(period=self.configuration.period, task=monitor_run).run,
                name="MonitorRunTask",
            )
            self.watched_runs[run.run_id] = monitor_run

        finished_runs: list[str] = []
        for key, monitor_run in self.watched_runs.items():
            if monitor_run.is_done:
                finished_runs.append(key)
        for finished_run_id in finished_runs:
            LOGGER.debug("Run %s finished", finished_run_id)
            self.watched_runs.pop(finished_run_id)

    async def _get_new_synapse_runs(
        self,
        current_dt: datetime,
        previous_dt: datetime,
    ) -> AsyncGenerator[SynapseRunData, None]:
        filters = RunFilterParameters(
            last_updated_after=previous_dt,
            last_updated_before=current_dt,
        )
        if pipelines := self.configuration.pipelines_filter:
            filters.filters = [
                RunQueryFilter(
                    operand=RunQueryFilterOperand.PIPELINE_NAME,
                    operator=RunQueryFilterOperator.IN,
                    values=pipelines,
                ),
            ]
        list_more = True
        while list_more:
            query_result = await artifacts_client().pipeline_run.query_pipeline_runs_by_workspace(filters)
            for run in query_result.value:
                if not run.pipeline_name or not run.run_id:  # type: ignore[unreachable]
                    LOGGER.error("Pipeline run does not have all required fields set, skipping - %s", run)
                elif run.run_id not in self.watched_runs:  # type: ignore[unreachable]
                    yield SynapseRunData(pipeline_name=run.pipeline_name, run_id=run.run_id)

            filters.continuation_token = query_result.continuation_token
            list_more = query_result.continuation_token is not None
