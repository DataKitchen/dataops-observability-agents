import pytest
from azure.core.exceptions import ResourceNotFoundError
from azure.synapse.artifacts.models import (
    PipelineRun,
    PipelineRunInvokedBy,
)

from agents.synapse_analytics.constants import COMPONENT_TOOL
from agents.synapse_analytics.monitor_run_task import MonitorRunTask
from toolkit.observability import EVENT_TYPE_KEY, EventType, Status


@pytest.fixture()
def run_response(synapse_client, run_data, earlier, now):
    invoked_by = PipelineRunInvokedBy()
    invoked_by.name = "Tengil"
    invoked_by.id = "an id"
    invoked_by.invoked_by_type = "a type"

    pipeline_run = PipelineRun()
    pipeline_run.status = "InProgress"
    pipeline_run.run_id = run_data.run_id
    pipeline_run.run_start = earlier
    pipeline_run.run_end = now
    pipeline_run.parameters = {"param1": 1, "param2": 2}
    pipeline_run.invoked_by = invoked_by
    pipeline_run.duration_in_ms = 1000
    synapse_client.pipeline_run.get_pipeline_run.return_value = pipeline_run
    return pipeline_run


@pytest.mark.unit()
async def test_monitor_run_contextmanager(
    synapse_client,
    empty_activity_response,
    run_response,
    now,
    channel,
    run_data,
):
    monitor_run = MonitorRunTask(
        synapse_run=run_data,
        initial_start_time=now,
        outbound_channel=channel,
    )
    assert not monitor_run._channel_open
    async with monitor_run:
        assert monitor_run._channel_open
    assert not monitor_run._channel_open


@pytest.mark.unit()
async def test_monitor_run_starting(synapse_client, empty_activity_response, run_response, now, channel, run_data):
    monitor_run = MonitorRunTask(
        synapse_run=run_data,
        initial_start_time=now,
        outbound_channel=channel,
    )
    await monitor_run.__aenter__()
    await monitor_run.execute(now, now)

    assert not monitor_run.is_done
    channel.send.assert_called_once_with(
        {
            EVENT_TYPE_KEY: EventType.RUN_STATUS.value,
            "status": Status.RUNNING.value,
            "metadata": {
                "parameters": run_response.parameters,
                "invoked_by": run_response.invoked_by.as_dict(),
            },
            "event_timestamp": run_response.run_start.astimezone().isoformat(),
            "pipeline_key": run_data.pipeline_name,
            "run_key": run_data.run_id,
            "external_url": None,
            "component_tool": COMPONENT_TOOL,
            "pipeline_name": run_data.pipeline_name,
        },
    )


@pytest.mark.unit()
async def test_monitor_run_ended(synapse_client, empty_activity_response, run_response, now, channel, run_data):
    monitor_run = MonitorRunTask(
        synapse_run=run_data,
        initial_start_time=now,
        outbound_channel=channel,
    )
    monitor_run.status = Status.RUNNING
    run_response.status = "Succeeded"
    await monitor_run.__aenter__()

    await monitor_run.execute(now, now)
    channel.send.assert_not_called()
    await monitor_run.execute(now, now)

    assert monitor_run.is_done
    channel.send.assert_called_once_with(
        {
            EVENT_TYPE_KEY: EventType.RUN_STATUS.value,
            "status": Status.COMPLETED.value,
            "metadata": {
                "run_duration_ms": run_response.duration_in_ms,
            },
            "event_timestamp": run_response.run_end.astimezone().isoformat(),
            "pipeline_key": run_data.pipeline_name,
            "run_key": run_data.run_id,
            "external_url": None,
        },
    )


@pytest.mark.unit()
async def test_monitor_run_no_update(synapse_client, empty_activity_response, run_response, now, channel, run_data):
    monitor_run = MonitorRunTask(
        synapse_run=run_data,
        initial_start_time=now,
        outbound_channel=channel,
    )
    monitor_run.status = Status.RUNNING
    run_response.status = "InProgress"
    await monitor_run.__aenter__()

    await monitor_run.execute(now, now)
    channel.send.assert_not_called()
    assert not monitor_run.is_done


@pytest.mark.unit()
async def test_monitor_run_unknown_status(
    synapse_client,
    empty_activity_response,
    run_response,
    now,
    channel,
    run_data,
):
    monitor_run = MonitorRunTask(
        synapse_run=run_data,
        initial_start_time=now,
        outbound_channel=channel,
    )
    monitor_run.status = Status.RUNNING
    run_response.status = "Invalid"
    await monitor_run.__aenter__()

    await monitor_run.execute(now, now)
    channel.send.assert_not_called()
    assert not monitor_run.is_done
    assert monitor_run.status is Status.RUNNING


@pytest.mark.unit()
async def test_monitor_run_nonexistent(synapse_client, now, channel, run_data):
    synapse_client.pipeline_run.get_pipeline_run.side_effect = ResourceNotFoundError
    monitor_run = MonitorRunTask(
        synapse_run=run_data,
        initial_start_time=now,
        outbound_channel=channel,
    )
    await monitor_run.__aenter__()

    await monitor_run.execute(now, now)
    channel.send.assert_not_called()
    assert monitor_run.is_done
