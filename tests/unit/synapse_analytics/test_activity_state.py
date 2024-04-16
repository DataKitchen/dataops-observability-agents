import dataclasses
from unittest.mock import ANY, call

import pytest

from agents.synapse_analytics.activity_states import CopyActivityState, SynapseActivityState, create_activity_state
from toolkit.observability import EVENT_TYPE_KEY, EventType, Status


@pytest.fixture()
def activity_data2(activity_data):
    return dataclasses.replace(activity_data)


@pytest.mark.unit()
@pytest.mark.usefixtures("register_synapse_config")
def test_create_activity_state_default(channel, activity_data):
    activity_data.activity_type = "some type"
    activity_state = create_activity_state(activity_data, channel)
    assert type(activity_state) is SynapseActivityState


@pytest.mark.unit()
@pytest.mark.usefixtures("register_synapse_config")
def test_create_activity_state_copy(channel, activity_data):
    activity_data.activity_type = "Copy"
    activity_state = create_activity_state(activity_data, channel)
    assert type(activity_state) is CopyActivityState


@pytest.mark.unit()
async def test_activity_state_starting(channel, activity_data, activity_data2, now, register_synapse_config):
    activity_data2.status = "InProgress"
    activity_data2.activity_run_start = now

    activity_state = SynapseActivityState(
        activity_data=activity_data,
        outbound_channel=channel,
    )
    await activity_state.update(activity_data2)
    assert activity_state.status is Status.RUNNING
    channel.send.assert_called_once_with(
        {
            EVENT_TYPE_KEY: EventType.RUN_STATUS.value,
            "status": Status.RUNNING.value,
            "metadata": {
                "activity_run_id": activity_data.activity_run_id,
                "activity_type": activity_data.activity_type,
                "activity_input": activity_data.input_,
            },
            "event_timestamp": activity_data2.activity_run_start.astimezone().isoformat(),
            "pipeline_key": activity_data.pipeline_name,
            "run_key": activity_data.pipeline_run_id,
            "task_key": activity_data.activity_name,
            "task_name": activity_data.activity_name,
            "external_url": None,
        },
    )


@pytest.mark.unit()
async def test_activity_state_continuing(channel, activity_data, activity_data2, now, register_synapse_config):
    activity_data2.status = "InProgress"

    activity_state = SynapseActivityState(
        activity_data=activity_data,
        outbound_channel=channel,
    )
    activity_state.status = Status.RUNNING

    await activity_state.update(activity_data2)
    channel.send.assert_not_called()


@pytest.mark.unit()
async def test_activity_state_ended(channel, activity_data, activity_data2, now, register_synapse_config):
    activity_data2.status = "Failed"
    activity_data2.activity_run_end = now

    activity_state = SynapseActivityState(
        activity_data=activity_data,
        outbound_channel=channel,
    )
    activity_state.status = Status.RUNNING

    await activity_state.update(activity_data2)
    assert activity_state.status is Status.FAILED
    channel.send.assert_called_once_with(
        {
            EVENT_TYPE_KEY: EventType.RUN_STATUS.value,
            "status": Status.FAILED.value,
            "metadata": {
                "activity_run_id": activity_data.activity_run_id,
                "activity_type": activity_data.activity_type,
                "activity_output": activity_data.output,
            },
            "event_timestamp": activity_data2.activity_run_end.astimezone().isoformat(),
            "pipeline_key": activity_data.pipeline_name,
            "run_key": activity_data.pipeline_run_id,
            "task_key": activity_data.activity_name,
            "external_url": None,
        },
    )


@pytest.mark.unit()
async def test_activity_state_ended_with_error_message(
    channel,
    activity_data,
    activity_data2,
    now,
    register_synapse_config,
):
    activity_data2.status = "Failed"
    activity_data2.activity_run_end = now
    activity_data2.error = {"message": "the error msg"}

    activity_state = SynapseActivityState(
        activity_data=activity_data,
        outbound_channel=channel,
    )
    activity_state.status = Status.RUNNING

    await activity_state.update(activity_data2)
    assert activity_state.status is Status.FAILED
    base = {
        "event_timestamp": activity_data2.activity_run_end.astimezone().isoformat(),
        "pipeline_key": activity_data.pipeline_name,
        "run_key": activity_data.pipeline_run_id,
        "task_key": activity_data.activity_name,
        "external_url": None,
    }
    channel.send.assert_has_calls(
        [
            call(
                {
                    EVENT_TYPE_KEY: EventType.MESSAGE_LOG.value,
                    "message": activity_data2.error["message"],
                    "log_level": "ERROR",
                    "metadata": ANY,
                    **base,
                },
            ),
            call(
                {
                    EVENT_TYPE_KEY: EventType.RUN_STATUS.value,
                    "status": Status.FAILED.value,
                    "metadata": {
                        "activity_run_id": activity_data.activity_run_id,
                        "activity_type": activity_data.activity_type,
                        "activity_output": activity_data.output,
                    },
                    **base,
                },
            ),
        ],
    )


@pytest.mark.unit()
async def test_activity_state_queued(channel, activity_data, activity_data2, now, register_synapse_config):
    activity_data2.status = "Queued"

    activity_state = SynapseActivityState(
        activity_data=activity_data,
        outbound_channel=channel,
    )
    await activity_state.update(activity_data2)
    assert activity_state.status is Status.UNKNOWN
    channel.send.assert_not_called()


@pytest.mark.unit()
async def test_activity_state_copy_type_ended_successfully(
    channel,
    activity_data,
    activity_data2,
    now,
    register_synapse_config,
):
    activity_data.activity_type = "Copy"
    activity_data2.status = "Succeeded"
    activity_data2.activity_run_end = now
    activity_data2.activity_type = "Copy"
    activity_data2.additional_properties = {"userProperties": {"Source": "m", "Destination": "n"}}

    activity_state = CopyActivityState(
        activity_data=activity_data,
        outbound_channel=channel,
    )
    activity_state.status = Status.RUNNING

    await activity_state.update(activity_data2)
    assert activity_state.status is Status.COMPLETED
    dataset_base = {
        "event_timestamp": activity_data2.activity_run_end.astimezone().isoformat(),
        "external_url": None,
    }
    channel.send.assert_has_calls(
        [
            call(
                {
                    EVENT_TYPE_KEY: EventType.DATASET_OPERATION.value,
                    "operation": "READ",
                    "metadata": {
                        "pipeline_name": activity_data.pipeline_name,
                        "pipeline_run_id": activity_data.pipeline_run_id,
                        "activity_name": activity_data.activity_name,
                        "activity_run_id": activity_data.activity_run_id,
                    },
                    "dataset_key": activity_data2.additional_properties["userProperties"]["Source"],
                    **dataset_base,
                },
            ),
            call(
                {
                    EVENT_TYPE_KEY: EventType.DATASET_OPERATION.value,
                    "operation": "WRITE",
                    "metadata": {
                        "pipeline_name": activity_data.pipeline_name,
                        "pipeline_run_id": activity_data.pipeline_run_id,
                        "activity_name": activity_data.activity_name,
                        "activity_run_id": activity_data.activity_run_id,
                    },
                    "dataset_key": activity_data2.additional_properties["userProperties"]["Destination"],
                    **dataset_base,
                },
            ),
            call(
                {
                    EVENT_TYPE_KEY: EventType.RUN_STATUS.value,
                    "status": Status.COMPLETED.value,
                    "metadata": {
                        "activity_run_id": activity_data.activity_run_id,
                        "activity_type": activity_data.activity_type,
                        "activity_output": activity_data.output,
                    },
                    "event_timestamp": activity_data2.activity_run_end.astimezone().isoformat(),
                    "pipeline_key": activity_data.pipeline_name,
                    "run_key": activity_data.pipeline_run_id,
                    "task_key": activity_data.activity_name,
                    "external_url": None,
                },
            ),
        ],
    )


@pytest.mark.unit()
async def test_activity_state_copy_type_ended_failed(
    channel,
    activity_data,
    activity_data2,
    now,
    register_synapse_config,
):
    activity_data.activity_type = "Copy"
    activity_data2.status = "Failed"
    activity_data2.activity_run_end = now
    activity_data2.activity_type = "Copy"
    activity_data2.additional_properties = {"userProperties": {"Source": "s", "Destination": "d"}}

    activity_state = CopyActivityState(
        activity_data=activity_data,
        outbound_channel=channel,
    )
    activity_state.status = Status.RUNNING

    await activity_state.update(activity_data2)
    assert activity_state.status is Status.FAILED
    channel.send.assert_called_once_with(
        {
            EVENT_TYPE_KEY: EventType.RUN_STATUS.value,
            "status": Status.FAILED.value,
            "metadata": {
                "activity_run_id": activity_data.activity_run_id,
                "activity_type": activity_data.activity_type,
                "activity_output": activity_data.output,
            },
            "event_timestamp": activity_data2.activity_run_end.astimezone().isoformat(),
            "pipeline_key": activity_data.pipeline_name,
            "run_key": activity_data.pipeline_run_id,
            "task_key": activity_data.activity_name,
            "external_url": None,
        },
    )
