import logging
from collections.abc import AsyncGenerator
from datetime import datetime

from azure.synapse.artifacts.models import (
    ActivityRun,
    ActivityRunsQueryResponse,
    RunFilterParameters,
)
from trio import MemorySendChannel

from toolkit.more_typing import JSON_DICT

from .activity_states import SynapseActivityStateBase, create_activity_state
from .client import artifacts_client
from .types import SynapseActivityData, SynapseRunData

LOGGER = logging.getLogger(__name__)


class MonitorActivities:
    synapse_run: SynapseRunData
    initial_start_time: datetime | None
    watched_activities: dict[str, SynapseActivityStateBase]

    def __init__(
        self,
        synapse_run: SynapseRunData,
        initial_start_time: datetime,
        outbound_channel: MemorySendChannel[JSON_DICT],
    ) -> None:
        self.outbound_channel = outbound_channel
        self.synapse_run = synapse_run
        self.initial_start_time = initial_start_time
        self.watched_activities = {}

    async def _get_activities(self, current_dt: datetime, previous_dt: datetime) -> AsyncGenerator[ActivityRun, None]:
        last_updated_after = previous_dt
        if self.initial_start_time:
            last_updated_after = self.initial_start_time
            self.initial_start_time = None
        list_more = True

        filters = RunFilterParameters(last_updated_after=last_updated_after, last_updated_before=current_dt)
        while list_more:
            query_result: ActivityRunsQueryResponse = await artifacts_client().pipeline_run.query_activity_runs(
                pipeline_name=self.synapse_run.pipeline_name,
                run_id=self.synapse_run.run_id,
                filter_parameters=filters,
            )
            for activity in query_result.value:
                yield activity
            filters.continuation_token = query_result.continuation_token
            list_more = query_result.continuation_token is not None

    def _get_activity_state(self, activity_data: SynapseActivityData) -> SynapseActivityStateBase:
        if (activity_state := self.watched_activities.get(activity_data.activity_name)) is None:
            LOGGER.info(
                "Run %s, new activity - name: %s, type: %s, id: %s",
                activity_data.pipeline_run_id,
                activity_data.activity_name,
                activity_data.activity_type,
                activity_data.activity_run_id,
            )
            activity_state = create_activity_state(
                activity_data=activity_data,
                outbound_channel=self.outbound_channel,
            )
            self.watched_activities[activity_data.activity_name] = activity_state
        return activity_state

    async def update(self, current_dt: datetime, previous_dt: datetime) -> None:
        async for activity_run in self._get_activities(current_dt, previous_dt):
            try:
                activity_data = SynapseActivityData.create(activity_run)
                activity_state = self._get_activity_state(activity_data)
                await activity_state.update(activity_data)
            except ValueError:
                LOGGER.exception("Activity run not valid, skipping - %s", activity_run)

        finished_activities: list[str] = []
        for name, activity in self.watched_activities.items():
            if activity.finished:
                finished_activities.append(name)
        for finished_activity in finished_activities:
            LOGGER.info("Activity %s finished", finished_activity)
            self.watched_activities.pop(finished_activity)
