import copy
from unittest.mock import patch

import pytest
from httpx import Response

from agents.powerbi.handles import (
    PowerBIListDatasetsEndpoint,
    PowerBIListGroupsEndpoint,
)
from agents.powerbi.tasks import GenericApiError, PowerBIDataset, PowerBIMonitorRunTask


@pytest.mark.unit()
@pytest.mark.usefixtures("register_powerbi_config", "register_basic_oauth_config")
async def test_get_groups_200(fetch_runs_task, groups, groups_data, mock_list_groups_response) -> None:
    result = await fetch_runs_task.get_groups()
    assert result == groups


@pytest.mark.unit()
@pytest.mark.usefixtures("register_powerbi_config", "register_basic_oauth_config")
async def test_filter_powerbi_groups_config(
    fetch_runs_task,
    groups,
    groups_data,
    mock_list_groups_response,
    powerbi_configuration,
) -> None:
    config = copy.deepcopy(powerbi_configuration)
    config.groups = ["group1", "group2"]
    fetch_runs_task.configuration = config
    result = await fetch_runs_task.get_groups()
    assert result == groups[1:3]


@pytest.mark.unit()
@pytest.mark.usefixtures("register_powerbi_config", "register_basic_oauth_config")
async def test_get_groups_non_200_response_raise(fetch_runs_task, powerbi_configuration) -> None:
    with patch.object(PowerBIListGroupsEndpoint, "handle", return_value=Response(status_code=502)):
        with pytest.raises(GenericApiError):
            await fetch_runs_task.get_groups()


@pytest.mark.unit()
@pytest.mark.usefixtures("register_powerbi_config", "register_basic_oauth_config")
async def test_get_datasets_200(
    dataset,
    dataset_data,
    fetch_runs_task,
    group,
    mock_datasets_response,
) -> None:
    result = await fetch_runs_task.get_datasets(group)
    assert result == [dataset]


@pytest.mark.unit()
@pytest.mark.usefixtures("register_powerbi_config", "register_basic_oauth_config")
async def test_get_datasets_non_200_raise(fetch_runs_task, group) -> None:
    with patch.object(PowerBIListDatasetsEndpoint, "handle", return_value=Response(status_code=502)):
        with pytest.raises(GenericApiError):
            await fetch_runs_task.get_datasets(group)


@pytest.mark.unit()
@pytest.mark.usefixtures("register_powerbi_config", "register_basic_oauth_config")
async def test_remove_groups(fetch_runs_task, groups) -> None:
    fetch_runs_task.groups_watched = {groups[1].group_id: groups[1]}
    assert groups[1].group_id in fetch_runs_task.groups_watched
    new_data = groups[2:]
    fetch_runs_task.remove_groups(new_data)
    assert groups[1].group_id not in fetch_runs_task.groups_watched


@pytest.mark.unit()
@pytest.mark.usefixtures("register_powerbi_config", "register_basic_oauth_config")
async def test_add_groups(fetch_runs_task, groups) -> None:
    fetch_runs_task.groups_watched = {groups[1].group_id: groups[1]}
    new_data = groups[1:]
    fetch_runs_task.add_groups(new_data)
    assert fetch_runs_task.groups_watched == {group.group_id: group for group in new_data}


@pytest.mark.unit()
@pytest.mark.usefixtures("register_powerbi_config", "register_basic_oauth_config")
async def test_remove_datasets(fetch_runs_task, groups, dataset) -> None:
    dataset2 = PowerBIDataset(dataset_id="d2", dataset_name="dataset2", is_refreshable=True)
    fetch_runs_task.datasets_watched = {
        groups[0].group_id: {dataset.dataset_id: dataset, dataset2.dataset_id: dataset2},
        groups[1].group_id: {dataset2.dataset_id: dataset2},
    }
    new_data = [dataset]
    fetch_runs_task.remove_datasets(groups[0].group_id, new_data)
    # remove dataset2 from group0 only; this should not affect group1
    assert fetch_runs_task.datasets_watched == {
        groups[0].group_id: {
            dataset.dataset_id: dataset,
        },
        groups[1].group_id: {dataset2.dataset_id: dataset2},
    }


@pytest.mark.unit()
@pytest.mark.usefixtures("register_powerbi_config", "register_basic_oauth_config")
async def test_add_datasets(dataset, fetch_runs_task, groups) -> None:
    fetch_runs_task.datasets_watched = {}
    dataset2 = PowerBIDataset(dataset_id="d2", dataset_name="dataset2")
    fetch_runs_task.add_datasets(groups[0].group_id, [dataset, dataset2])
    expected_result = {
        groups[0].group_id: {
            dataset.dataset_id: dataset,
            dataset2.dataset_id: dataset2,
        },
    }
    assert fetch_runs_task.datasets_watched == expected_result

    fetch_runs_task.add_datasets(groups[0].group_id, [dataset])
    # Data already existed, should not add again
    assert fetch_runs_task.datasets_watched == expected_result


@pytest.mark.unit()
@pytest.mark.usefixtures("register_powerbi_config", "register_basic_oauth_config")
async def test_end_monitor_tasks(group, dataset, fetch_runs_task, monitor_run_task) -> None:
    fetch_runs_task.groups_watched = {group.group_id: group}
    fetch_runs_task.datasets_watched = {group.group_id: {dataset.dataset_id: dataset}}
    fetch_runs_task.refresh_tasks_watched = [monitor_run_task]

    assert monitor_run_task.is_done is False
    fetch_runs_task.end_monitor_tasks()
    assert monitor_run_task.is_done is False

    fetch_runs_task.groups_watched = {}
    fetch_runs_task.end_monitor_tasks()
    assert monitor_run_task.is_done is True


@pytest.mark.unit()
@pytest.mark.usefixtures("register_powerbi_config", "register_basic_oauth_config")
async def test_end_monitor_tasks_2(group, dataset, fetch_runs_task, monitor_run_task) -> None:
    fetch_runs_task.groups_watched = {group.group_id: group}
    fetch_runs_task.datasets_watched = {group.group_id: {dataset.dataset_id: dataset}}
    fetch_runs_task.refresh_tasks_watched = [monitor_run_task]

    assert monitor_run_task.is_done is False
    fetch_runs_task.end_monitor_tasks()
    assert monitor_run_task.is_done is False

    dataset2 = PowerBIDataset(dataset_id="d2", dataset_name="dataset2")
    fetch_runs_task.datasets_watched = {group.group_id: {dataset2.dataset_id: dataset2}}
    fetch_runs_task.end_monitor_tasks()
    assert monitor_run_task.is_done is True


@pytest.mark.unit()
@pytest.mark.usefixtures("register_powerbi_config", "register_basic_oauth_config")
async def test_fetch_runs_task_execute(
    fetch_runs_task,
    groups_data,
    mock_list_groups_response,
    timestamp_now,
    timestamp_past,
) -> None:
    def get_datasets(*_, **kwargs):
        data = []
        if (key := kwargs["path_args"]["groupId"]) in ["g0", "g1"]:
            data.append({"id": "d_" + key, "name": "dataset_" + key, "isRefreshable": True, "webUrl": None})
        return Response(status_code=200, json={"value": data})

    with patch.object(PowerBIListDatasetsEndpoint, "handle", side_effect=get_datasets):
        await fetch_runs_task.execute(timestamp_now, timestamp_past)
        assert fetch_runs_task.nursery.start_soon.call_count == 2
        assert len(fetch_runs_task.refresh_tasks_watched) == 2
        for task in fetch_runs_task.refresh_tasks_watched:
            assert task.is_done is False
            assert type(task) is PowerBIMonitorRunTask
