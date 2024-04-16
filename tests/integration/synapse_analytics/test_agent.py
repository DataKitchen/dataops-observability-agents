from unittest.mock import Mock

import pytest
from azure.synapse.artifacts.models import (
    ActivityRunsQueryResponse,
)
from trio import move_on_after

from agents.synapse_analytics.constants import COMPONENT_TOOL
from agents.synapse_analytics.list_runs_task import ListRunsTask
from toolkit.observability import EventType, Status


@pytest.fixture()
async def _fail_on_timeout():
    """
    Fail the tests if enough time passed i.e. if the test got stuck.

    autojump_clock affect move_on_after so that the timeout doesn't take 30 seconds. However, autojump_clock must be
    explicitly used by the test function to work.
    """
    with move_on_after(30):  # noqa: TRIO100
        yield


@pytest.fixture()
def list_run_response(synapse_client, inprogress_run):
    response = Mock()
    response.continuation_token = None
    response.value = [inprogress_run]
    synapse_client.pipeline_run.query_pipeline_runs_by_workspace.return_value = response
    return response


@pytest.fixture()
def run_response(synapse_client, earlier, now, successful_run):
    synapse_client.pipeline_run.get_pipeline_run.return_value = successful_run
    return successful_run


@pytest.fixture()
def empty_activity_response(synapse_client):
    response = ActivityRunsQueryResponse(value=[])
    response.continuation_token = None
    synapse_client.pipeline_run.query_activity_runs.return_value = response
    return response


@pytest.mark.integration()
async def test_synapse_analytics_run_start_stop_simple(
    synapse_client,
    nursery,
    now,
    earlier,
    outbound_channel,
    inbound_channel,
    list_run_response,
    run_response,
    empty_activity_response,
    _fail_on_timeout,  # noqa: PT019
    autojump_clock,
):
    list_runs = ListRunsTask(nursery, outbound_channel)
    await list_runs.execute(now, earlier)

    assert await inbound_channel.receive() == {
        "EVENT_TYPE": EventType.RUN_STATUS.value,
        "status": Status.RUNNING.value,
        "metadata": {
            "parameters": run_response.parameters,
            "invoked_by": run_response.invoked_by.as_dict(),
        },
        "event_timestamp": earlier.astimezone().isoformat(),
        "pipeline_key": list_run_response.value[0].pipeline_name,
        "run_key": list_run_response.value[0].run_id,
        "external_url": None,
        "component_tool": COMPONENT_TOOL,
        "pipeline_name": list_run_response.value[0].pipeline_name,
    }
    assert await inbound_channel.receive() == {
        "EVENT_TYPE": EventType.RUN_STATUS.value,
        "status": Status.COMPLETED.value,
        "metadata": {
            "run_duration_ms": run_response.duration_in_ms,
        },
        "event_timestamp": now.astimezone().isoformat(),
        "pipeline_key": list_run_response.value[0].pipeline_name,
        "run_key": list_run_response.value[0].run_id,
        "external_url": None,
    }


@pytest.mark.integration()
async def test_synapse_analytics_run_start_stop_with_activities(
    synapse_client,
    nursery,
    now,
    earlier,
    outbound_channel,
    inbound_channel,
    list_run_response,
    inprogress_run,
    start_activity1,
    end_activity1,
    end_activity2,
    successful_run,
    run_response,
    _fail_on_timeout,  # noqa: PT019
    autojump_clock,
):
    synapse_client.pipeline_run.get_pipeline_run.side_effect = [
        inprogress_run,
        inprogress_run,
        inprogress_run,
        successful_run,
        successful_run,
    ]
    activity_responses = []
    for activity in [start_activity1, end_activity1, end_activity2]:
        activity_response = ActivityRunsQueryResponse(value=[activity])
        activity_response.continuation_token = None
        activity_responses.append(activity_response)
    empty_activity_response = ActivityRunsQueryResponse(value=[])
    empty_activity_response.continuation_token = None
    activity_responses.append(empty_activity_response)
    activity_responses.append(empty_activity_response)
    synapse_client.pipeline_run.query_activity_runs.side_effect = activity_responses

    list_runs = ListRunsTask(nursery, outbound_channel)
    await list_runs.execute(now, earlier)

    # Start run
    assert await inbound_channel.receive() == {
        "EVENT_TYPE": EventType.RUN_STATUS.value,
        "status": Status.RUNNING.value,
        "metadata": {
            "parameters": run_response.parameters,
            "invoked_by": run_response.invoked_by.as_dict(),
        },
        "event_timestamp": earlier.astimezone().isoformat(),
        "pipeline_key": list_run_response.value[0].pipeline_name,
        "run_key": list_run_response.value[0].run_id,
        "external_url": None,
        "component_tool": COMPONENT_TOOL,
        "pipeline_name": list_run_response.value[0].pipeline_name,
    }
    # Start activity1
    assert await inbound_channel.receive() == {
        "EVENT_TYPE": EventType.RUN_STATUS.value,
        "status": Status.RUNNING.value,
        "metadata": {
            "activity_run_id": start_activity1.activity_run_id,
            "activity_type": start_activity1.activity_type,
            "activity_input": start_activity1.input,
        },
        "event_timestamp": earlier.astimezone().isoformat(),
        "pipeline_key": list_run_response.value[0].pipeline_name,
        "run_key": list_run_response.value[0].run_id,
        "task_key": start_activity1.activity_name,
        "task_name": start_activity1.activity_name,
        "external_url": None,
    }
    # End activity1
    assert await inbound_channel.receive() == {
        "EVENT_TYPE": EventType.RUN_STATUS.value,
        "status": Status.COMPLETED.value,
        "metadata": {
            "activity_run_id": end_activity1.activity_run_id,
            "activity_type": end_activity1.activity_type,
            "activity_output": end_activity1.output,
        },
        "event_timestamp": now.astimezone().isoformat(),
        "pipeline_key": list_run_response.value[0].pipeline_name,
        "run_key": list_run_response.value[0].run_id,
        "task_key": end_activity1.activity_name,
        "external_url": None,
    }
    # Start activity2
    assert await inbound_channel.receive() == {
        "EVENT_TYPE": EventType.RUN_STATUS.value,
        "status": Status.RUNNING.value,
        "metadata": {
            "activity_run_id": end_activity2.activity_run_id,
            "activity_type": end_activity2.activity_type,
            "activity_input": end_activity2.input,
        },
        "event_timestamp": None,
        "pipeline_key": list_run_response.value[0].pipeline_name,
        "run_key": list_run_response.value[0].run_id,
        "task_key": end_activity2.activity_name,
        "task_name": end_activity2.activity_name,
        "external_url": None,
    }
    # End activity2
    assert await inbound_channel.receive() == {
        "EVENT_TYPE": EventType.RUN_STATUS.value,
        "status": Status.FAILED.value,
        "metadata": {
            "activity_run_id": end_activity2.activity_run_id,
            "activity_type": end_activity2.activity_type,
            "activity_output": end_activity2.output,
        },
        "event_timestamp": None,
        "pipeline_key": list_run_response.value[0].pipeline_name,
        "run_key": list_run_response.value[0].run_id,
        "task_key": end_activity2.activity_name,
        "external_url": None,
    }
    # End run
    assert await inbound_channel.receive() == {
        "EVENT_TYPE": EventType.RUN_STATUS.value,
        "status": Status.COMPLETED.value,
        "metadata": {
            "run_duration_ms": run_response.duration_in_ms,
        },
        "event_timestamp": now.astimezone().isoformat(),
        "pipeline_key": list_run_response.value[0].pipeline_name,
        "run_key": list_run_response.value[0].run_id,
        "external_url": None,
    }


@pytest.mark.integration()
async def test_synapse_analytics_copy_activity(
    synapse_client,
    nursery,
    now,
    earlier,
    outbound_channel,
    inbound_channel,
    list_run_response,
    copy_activity,
    successful_run,
    run_response,
    _fail_on_timeout,  # noqa: PT019
    autojump_clock,
):
    synapse_client.pipeline_run.get_pipeline_run.return_value = successful_run
    activity_responses = []
    for activities in [[copy_activity], []]:
        activity_response = ActivityRunsQueryResponse(value=activities)
        activity_response.continuation_token = None
        activity_responses.append(activity_response)
    synapse_client.pipeline_run.query_activity_runs.side_effect = activity_responses

    list_runs = ListRunsTask(nursery, outbound_channel)
    await list_runs.execute(now, earlier)

    # Start run
    assert await inbound_channel.receive() == {
        "EVENT_TYPE": EventType.RUN_STATUS.value,
        "status": Status.RUNNING.value,
        "metadata": {
            "parameters": run_response.parameters,
            "invoked_by": run_response.invoked_by.as_dict(),
        },
        "event_timestamp": earlier.astimezone().isoformat(),
        "pipeline_key": list_run_response.value[0].pipeline_name,
        "run_key": list_run_response.value[0].run_id,
        "external_url": None,
        "component_tool": COMPONENT_TOOL,
        "pipeline_name": list_run_response.value[0].pipeline_name,
    }
    # Start activity1
    assert await inbound_channel.receive() == {
        "EVENT_TYPE": EventType.RUN_STATUS.value,
        "status": Status.RUNNING.value,
        "metadata": {
            "activity_run_id": copy_activity.activity_run_id,
            "activity_type": copy_activity.activity_type,
            "activity_input": copy_activity.input,
        },
        "event_timestamp": None,
        "pipeline_key": list_run_response.value[0].pipeline_name,
        "run_key": list_run_response.value[0].run_id,
        "task_key": copy_activity.activity_name,
        "task_name": copy_activity.activity_name,
        "external_url": None,
    }
    # Dataset read
    assert await inbound_channel.receive() == {
        "EVENT_TYPE": EventType.DATASET_OPERATION.value,
        "operation": "READ",
        "metadata": {
            "pipeline_name": list_run_response.value[0].pipeline_name,
            "pipeline_run_id": list_run_response.value[0].run_id,
            "activity_name": copy_activity.activity_name,
            "activity_run_id": copy_activity.activity_run_id,
        },
        "event_timestamp": None,
        "dataset_key": copy_activity.additional_properties["userProperties"]["Source"],
        "external_url": None,
    }
    # Dataset write
    assert await inbound_channel.receive() == {
        "EVENT_TYPE": EventType.DATASET_OPERATION.value,
        "operation": "WRITE",
        "metadata": {
            "pipeline_name": list_run_response.value[0].pipeline_name,
            "pipeline_run_id": list_run_response.value[0].run_id,
            "activity_name": copy_activity.activity_name,
            "activity_run_id": copy_activity.activity_run_id,
        },
        "event_timestamp": None,
        "dataset_key": copy_activity.additional_properties["userProperties"]["Destination"],
        "external_url": None,
    }
    # End activity2
    assert await inbound_channel.receive() == {
        "EVENT_TYPE": EventType.RUN_STATUS.value,
        "status": Status.COMPLETED.value,
        "metadata": {
            "activity_run_id": copy_activity.activity_run_id,
            "activity_type": copy_activity.activity_type,
            "activity_output": copy_activity.output,
        },
        "event_timestamp": None,
        "pipeline_key": list_run_response.value[0].pipeline_name,
        "run_key": list_run_response.value[0].run_id,
        "task_key": copy_activity.activity_name,
        "external_url": None,
    }
    # End run
    assert await inbound_channel.receive() == {
        "EVENT_TYPE": EventType.RUN_STATUS.value,
        "status": Status.COMPLETED.value,
        "metadata": {
            "run_duration_ms": run_response.duration_in_ms,
        },
        "event_timestamp": now.astimezone().isoformat(),
        "pipeline_key": list_run_response.value[0].pipeline_name,
        "run_key": list_run_response.value[0].run_id,
        "external_url": None,
    }
