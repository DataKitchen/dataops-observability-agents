import pytest
from azure.synapse.artifacts.models import (
    ActivityRun,
    ActivityRunsQueryResponse,
)

from agents.synapse_analytics.activity_states import SynapseActivityState
from agents.synapse_analytics.monitor_activities import MonitorActivities
from toolkit.observability import Status


@pytest.fixture()
def activity_run2():
    activity_run = ActivityRun()
    activity_run.activity_name = "activity name 2"
    activity_run.activity_type = "activity type 2"
    activity_run.activity_run_id = "activity id 2"
    activity_run.pipeline_name = "a name 2"
    activity_run.pipeline_run_id = "an id 2"
    activity_run.status = "InProgress"
    return activity_run


@pytest.fixture()
def activity_response(empty_activity_response, activity_run):
    empty_activity_response.value.append(activity_run)
    return empty_activity_response


@pytest.fixture()
def activity_state(activity_run, channel, activity_data):
    return SynapseActivityState(
        activity_data=activity_data,
        outbound_channel=channel,
    )


@pytest.mark.unit()
async def test_monitor_activities_new_activity(synapse_client, activity_response, now, channel, run_data, activity_run):
    monitor_activities = MonitorActivities(
        synapse_run=run_data,
        initial_start_time=now,
        outbound_channel=channel,
    )
    await monitor_activities.update(now, now)
    assert len(monitor_activities.watched_activities) == 1
    assert (
        monitor_activities.watched_activities[activity_run.activity_name].activity_data.activity_run_id
        == activity_run.activity_run_id
    )


@pytest.mark.unit()
async def test_monitor_activities_already_watched_activity(
    synapse_client,
    activity_response,
    now,
    channel,
    run_data,
    activity_run,
    activity_state,
):
    monitor_activities = MonitorActivities(
        synapse_run=run_data,
        initial_start_time=now,
        outbound_channel=channel,
    )
    monitor_activities.watched_activities[activity_run.activity_name] = activity_state
    assert activity_state.status is not Status.RUNNING

    await monitor_activities.update(now, now)

    assert len(monitor_activities.watched_activities) == 1
    watched_state = monitor_activities.watched_activities[activity_run.activity_name]
    assert watched_state.activity_data.activity_run_id == activity_run.activity_run_id
    assert watched_state.status is Status.RUNNING


@pytest.mark.unit()
async def test_monitor_activities_finished_activity(
    synapse_client,
    activity_response,
    now,
    channel,
    run_data,
    activity_run,
    activity_state,
):
    monitor_activities = MonitorActivities(
        synapse_run=run_data,
        initial_start_time=now,
        outbound_channel=channel,
    )
    activity_run.status = "Succeeded"
    activity_state.status = Status.COMPLETED
    monitor_activities.watched_activities[activity_run.activity_name] = activity_state

    await monitor_activities.update(now, now)

    assert len(monitor_activities.watched_activities) == 0


@pytest.mark.unit()
async def test_monitor_activities_invalid_activity(
    synapse_client,
    activity_response,
    now,
    channel,
    run_data,
    activity_run,
    activity_state,
):
    monitor_activities = MonitorActivities(
        synapse_run=run_data,
        initial_start_time=now,
        outbound_channel=channel,
    )
    # activity_type, among others, must be set
    activity_run.activity_type = None

    await monitor_activities.update(now, now)
    assert len(monitor_activities.watched_activities) == 0


@pytest.mark.unit()
async def test_monitor_activities_paged_activities(
    synapse_client,
    empty_activity_response,
    now,
    channel,
    run_data,
    activity_run,
    activity_run2,
    activity_state,
):
    continuation_response = ActivityRunsQueryResponse(value=[activity_run])
    continuation_response.continuation_token = "def"
    continuation_response.value = [activity_run]
    empty_activity_response.value = [activity_run2]
    synapse_client.pipeline_run.query_activity_runs.side_effect = [continuation_response, empty_activity_response]

    monitor_activities = MonitorActivities(
        synapse_run=run_data,
        initial_start_time=now,
        outbound_channel=channel,
    )

    await monitor_activities.update(now, now)
    assert len(monitor_activities.watched_activities) == 2
