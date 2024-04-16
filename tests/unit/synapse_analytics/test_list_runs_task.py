from unittest.mock import Mock, patch

import pytest
from azure.synapse.artifacts.models import (
    PipelineRun,
    RunFilterParameters,
    RunQueryFilter,
    RunQueryFilterOperand,
    RunQueryFilterOperator,
)

from agents.synapse_analytics.config import (
    SynapseAnalyticsConfiguration,
)
from agents.synapse_analytics.list_runs_task import ListRunsTask
from agents.synapse_analytics.monitor_run_task import MonitorRunTask
from agents.synapse_analytics.types import SynapseRunData
from framework.core.channels import NullSendChannel
from registry import ConfigurationRegistry


@pytest.fixture()
def pipeline_run():
    pipeline_run = PipelineRun()
    pipeline_run.pipeline_name = "a name"
    pipeline_run.run_id = "a id"
    return pipeline_run


@pytest.fixture()
def pipeline_run2():
    pipeline_run = PipelineRun()
    pipeline_run.pipeline_name = "two name"
    pipeline_run.run_id = "two id"
    return pipeline_run


@pytest.fixture()
def list_response(synapse_client):
    response = Mock()
    response.continuation_token = None
    response.value = []
    synapse_client.pipeline_run.query_pipeline_runs_by_workspace.return_value = response
    return response


@pytest.fixture()
def list_nursery(nursery):
    nursery.start_soon = Mock()
    return nursery


@pytest.fixture()
def patch_pipline_filter():
    with patch.dict("os.environ", {"DK_SYNAPSE_ANALYTICS_PIPELINES_FILTER": '["pipeline name"]'}) as f:
        yield f


@pytest.mark.unit()
async def test_list_runs_no_new_run(
    synapse_client,
    list_response,
    list_nursery,
    earlier,
    now,
    env_synapse_config,
    mock_core_env_vars,
    patch_pipline_filter,
):
    # Ugh, it's registered by synapse_client fixture so need to reregister
    ConfigurationRegistry.__initialized_configurations__.clear()
    ConfigurationRegistry().register("synapse_analytics", SynapseAnalyticsConfiguration)
    list_runs = ListRunsTask(list_nursery, NullSendChannel())
    await list_runs.execute(now, earlier)

    synapse_client.pipeline_run.query_pipeline_runs_by_workspace.assert_called_once_with(
        RunFilterParameters(
            last_updated_after=earlier,
            last_updated_before=now,
            filters=[
                RunQueryFilter(
                    operand=RunQueryFilterOperand.PIPELINE_NAME,
                    operator=RunQueryFilterOperator.IN,
                    values=["pipeline name"],
                ),
            ],
        ),
    )
    list_nursery.start_soon.assert_not_called()
    assert len(list_runs.watched_runs) == 0


@pytest.mark.unit()
async def test_list_runs_two_new_runs(synapse_client, list_response, list_nursery, pipeline_run, pipeline_run2, now):
    list_response.value = [pipeline_run, pipeline_run2]
    list_runs = ListRunsTask(list_nursery, NullSendChannel())
    await list_runs.execute(now, now)

    assert list_nursery.start_soon.call_count == 2
    assert len(list_runs.watched_runs) == 2
    assert pipeline_run.run_id in list_runs.watched_runs
    assert pipeline_run2.run_id in list_runs.watched_runs


@pytest.mark.unit()
async def test_list_runs_two_pages(synapse_client, list_response, list_nursery, pipeline_run, pipeline_run2, now):
    continuation_response = Mock()
    continuation_response.continuation_token = "abc"
    continuation_response.value = [pipeline_run]
    list_response.value = [pipeline_run2]
    synapse_client.pipeline_run.query_pipeline_runs_by_workspace.side_effect = [continuation_response, list_response]

    list_runs = ListRunsTask(list_nursery, NullSendChannel())
    await list_runs.execute(now, now)

    assert list_nursery.start_soon.call_count == 2
    assert len(list_runs.watched_runs) == 2
    assert pipeline_run.run_id in list_runs.watched_runs
    assert pipeline_run2.run_id in list_runs.watched_runs


@pytest.mark.unit()
async def test_list_runs_invalid_run(synapse_client, list_response, list_nursery, pipeline_run, now):
    pipeline_run.run_id = None
    list_response.value = [pipeline_run]

    list_runs = ListRunsTask(list_nursery, NullSendChannel())
    await list_runs.execute(now, now)

    list_nursery.start_soon.assert_not_called()
    assert len(list_runs.watched_runs) == 0


@pytest.mark.unit()
async def test_list_runs_already_watched(synapse_client, list_response, list_nursery, pipeline_run, now):
    list_response.value = [pipeline_run]

    list_runs = ListRunsTask(list_nursery, NullSendChannel())
    list_runs.watched_runs[pipeline_run.run_id] = MonitorRunTask(
        synapse_run=SynapseRunData(pipeline_name=pipeline_run.pipeline_name, run_id=pipeline_run.run_id),
        initial_start_time=now,
        outbound_channel=NullSendChannel(),
    )
    await list_runs.execute(now, now)

    list_nursery.start_soon.assert_not_called()
    assert len(list_runs.watched_runs) == 1
    assert pipeline_run.run_id in list_runs.watched_runs


@pytest.mark.unit()
async def test_list_runs_remove_finished(synapse_client, list_response, list_nursery, pipeline_run, now):
    list_runs = ListRunsTask(list_nursery, NullSendChannel())
    monitor_run = MonitorRunTask(
        synapse_run=SynapseRunData(pipeline_name=pipeline_run.pipeline_name, run_id=pipeline_run.run_id),
        initial_start_time=now,
        outbound_channel=NullSendChannel(),
    )
    monitor_run.finish()
    list_runs.watched_runs[pipeline_run.run_id] = monitor_run
    await list_runs.execute(now, now)

    list_nursery.start_soon.assert_not_called()
    assert len(list_runs.watched_runs) == 0
