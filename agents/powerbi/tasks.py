import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from http import HTTPStatus
from types import TracebackType
from typing import Any, Self, cast

from httpx import URL
from trio import MemorySendChannel, Nursery

from framework.configuration.http import HTTPClientConfig
from framework.core.handles import get_client
from framework.core.loops import PeriodicLoop
from framework.core.tasks import PeriodicTask
from registry import ConfigurationRegistry
from registry.configuration_auth_credentials import load_auth_class
from toolkit.more_typing import JSON_DICT
from toolkit.observability import Status

from .config import PowerBIConfiguration
from .constants import ERROR_MESSAGE_DICT, POWERBI_DEFAULT_SCOPE, STATUS_TO_LOG_LEVEL
from .handles import (
    PowerBIListDatasetRefreshEndpoint,
    PowerBIListDatasetsEndpoint,
    PowerBIListGroupsEndpoint,
    PowerBIListReportsEndpoint,
)
from .helpers import get_status, send_dataset_operation_event, send_message_log_event, send_run_status_event

LOGGER = logging.getLogger(__name__)


class GenericApiError(Exception):
    pass


@dataclass(kw_only=True)
class PowerBIGroup:
    group_id: str
    group_name: str


@dataclass(kw_only=True)
class PowerBIDataset:
    dataset_id: str
    dataset_name: str
    is_refreshable: bool = True
    web_url: str | None = None
    group_id: str | None = None


@dataclass(kw_only=True)
class PowerBIDatasetRefresh:
    request_id: str
    refresh_type: str
    status: str
    start_time: str
    end_time: str | None = None
    exception_json: str | None = None


@dataclass(kw_only=True)
class PowerBIReport:
    report_id: str
    report_name: str
    report_type: str
    dataset_id: str
    web_url: str | None = None


class PowerBIFetchDatasetsTask(PeriodicTask):
    """
    Fetching PowerBI datasets periodically. Note: a PowerBI dateset is equivalent to a `pipeline` in Observability.

    When a new dataset found, the task creates a PowerBIMonitorRunTask to start monitoring the dataset refresh activity.

    It also watches for relevant PowerBI resources in this context, including groups, datasets, and monitor tasks.

    When a group or a dataset is removed from users' access, this task will stop all the monitor tasks that are
    related to the removed resources.
    """

    def __init__(
        self,
        nursery: Nursery,
        outbound_channel: MemorySendChannel[JSON_DICT],
        **kwargs: Any,
    ):
        super().__init__(outbound_channel=outbound_channel, **kwargs)
        self.registry = ConfigurationRegistry()
        self.configuration = ConfigurationRegistry().lookup("powerbi", PowerBIConfiguration)
        auth = load_auth_class(POWERBI_DEFAULT_SCOPE)
        self.http_config = self.registry.mutate("http", HTTPClientConfig, auth=auth)
        self.client = get_client(self.http_config)
        self.group_endpoint = PowerBIListGroupsEndpoint(
            base_url=URL(str(self.configuration.base_api_url)),
            client=self.client,
        )
        self.dataset_endpoint = PowerBIListDatasetsEndpoint(
            base_url=URL(str(self.configuration.base_api_url)),
            client=self.client,
        )
        self.nursery = nursery
        self.groups_watched: dict[str, Any] = {}
        self.datasets_watched: dict[str, Any] = {}
        self.refresh_tasks_watched: list[PowerBIMonitorRunTask] = []

    async def __aenter__(self) -> Self:
        await self.client.__aenter__()
        return await super().__aenter__()

    async def __aexit__(self, exc_type: type[BaseException], exc_val: BaseException, exc_tb: TracebackType) -> None:
        await self.client.__aexit__(exc_type, exc_val, exc_tb)
        return await super().__aexit__(exc_type, exc_val, exc_tb)

    async def get_groups(self) -> list[PowerBIGroup]:
        response = await self.group_endpoint.handle()
        if response.status_code == HTTPStatus.OK:
            data = response.json()

            # Get all groups or filter by groups that are defined by the users
            if self.configuration.groups:
                groups = [
                    PowerBIGroup(group_id=g["id"], group_name=g["name"])
                    for g in data["value"]
                    if g["name"] in self.configuration.groups
                ]
            else:
                groups = [PowerBIGroup(group_id=g["id"], group_name=g["name"]) for g in data["value"]]
            return groups
        else:
            raise GenericApiError("Failed to fetch groups", response)

    def remove_groups(self, data: list[PowerBIGroup]) -> None:
        """Remove groups from groups_watched if groups no longer exist in the latest data fetched"""
        new_ids = [g.group_id for g in data]
        to_del = [group_id for group_id in self.groups_watched.keys() if group_id not in new_ids]
        for key in to_del:
            del self.groups_watched[key]

    def add_groups(self, data: list[PowerBIGroup]) -> None:
        for group in data:
            if group.group_id not in self.groups_watched:
                LOGGER.debug("New group found: %s", group.group_name)
                self.groups_watched[group.group_id] = group

    async def get_datasets(self, group_id: str) -> list:
        response = await self.dataset_endpoint.handle(path_args={"groupId": group_id})
        if response.status_code == HTTPStatus.OK:
            data = response.json()
            datasets = [
                PowerBIDataset(
                    dataset_id=d["id"],
                    dataset_name=d["name"],
                    is_refreshable=d["isRefreshable"],
                    web_url=d["webUrl"],
                )
                for d in data["value"]
                if d["isRefreshable"]
            ]
            return datasets
        else:
            raise GenericApiError(f"Failed to fetch datasets for {group_id} group: {response}")

    def remove_datasets(self, group_id: str, data: list) -> None:
        """Remove datasets from datasets_watched if they no longer exist in the latest data fetched"""
        new_ids = {dataset.dataset_id for dataset in data}
        if (current := self.datasets_watched.get(group_id, None)) is None:
            return
        to_del = [dataset_id for dataset_id in current.keys() if dataset_id not in new_ids]
        for key in to_del:
            del current[key]

    def add_datasets(self, group_id: str, data: list[PowerBIDataset]) -> list[PowerBIDataset]:
        new_datasets = []
        if self.datasets_watched.get(group_id, None) is None:
            self.datasets_watched[group_id] = {}
        for dataset in data:
            if dataset.dataset_id not in self.datasets_watched[group_id]:
                LOGGER.debug("New dataset found: %s", dataset.dataset_name)
                self.datasets_watched[group_id].update({dataset.dataset_id: dataset})
                new_datasets.append(dataset)
        return new_datasets

    def end_monitor_tasks(self) -> None:
        """End monitor tasks when groups or datasets no longer exist in the latest data fetched"""
        watched_datasets = {
            dataset_id for dataset_dict in self.datasets_watched.values() for dataset_id in dataset_dict.keys()
        }
        for task in self.refresh_tasks_watched:
            ready_to_finish = (
                task.group_id not in self.groups_watched or task.dataset.dataset_id not in watched_datasets
            )
            if not task.is_done and ready_to_finish:
                LOGGER.debug("Ending monitor task: %s - %s", task.group_id, task.dataset.dataset_name)
                task.finish()

    async def execute(self, current_dt: datetime, previous_dt: datetime) -> None:
        groups = await self.get_groups()
        self.remove_groups(groups)
        self.add_groups(groups)
        for group_id in self.groups_watched.keys():
            datasets = await self.get_datasets(group_id)
            self.remove_datasets(group_id, datasets)
            new_datasets = self.add_datasets(group_id, datasets)
            for dataset in new_datasets:
                get_dataset_refresh_task = PowerBIMonitorRunTask(
                    # Mypy bug; see https://github.com/python/mypy/issues/16659
                    outbound_channel=cast(MemorySendChannel[JSON_DICT], self.outbound_channel.clone()),
                    nursery=self.nursery,
                    group_id=group_id,
                    dataset=dataset,
                )
                self.nursery.start_soon(
                    PeriodicLoop(period=self.configuration.period, task=get_dataset_refresh_task).run,
                )
                self.refresh_tasks_watched.append(get_dataset_refresh_task)

        self.end_monitor_tasks()


class PowerBIMonitorRunTask(PeriodicTask):
    """
    Polling for dataset refresh activity and report its status to the Observability app accordingly.

    Note: a dataset refresh is equivalent to a `run` in Observability.

    This task monitors the dataset refresh activity until the dataset is removed from the user access.
    """

    def __init__(
        self,
        nursery: Nursery,
        outbound_channel: MemorySendChannel[JSON_DICT],
        group_id: str,
        dataset: PowerBIDataset,
    ) -> None:
        super().__init__(outbound_channel=outbound_channel)
        self.registry = ConfigurationRegistry()
        self.configuration = self.registry.lookup("powerbi", PowerBIConfiguration)
        auth = load_auth_class(POWERBI_DEFAULT_SCOPE)
        self.http_config = self.registry.mutate("http", HTTPClientConfig, auth=auth)
        self.client = get_client(self.http_config)
        self.endpoint = PowerBIListDatasetRefreshEndpoint(
            base_url=URL(str(self.configuration.base_api_url)),
            client=self.client,
        )
        self.reports_endpoint = PowerBIListReportsEndpoint(
            base_url=URL(str(self.configuration.base_api_url)),
            client=self.client,
        )
        self.nursery = nursery
        self.group_id = group_id
        self.dataset = dataset
        self.status = Status.UNKNOWN
        self.start_time = datetime.now(UTC)
        self.finished_refreshes: list[str] = []

    async def report_status(self, refresh_data: PowerBIDatasetRefresh) -> None:
        metadata = {
            "dataset_refresh_id": refresh_data.request_id,
            "dataset_refresh_type": refresh_data.refresh_type,
            "dataset": vars(self.dataset),
        }
        pipeline_event_data = {
            "event_timestamp": refresh_data.start_time,
            "pipeline_key": self.dataset.dataset_id,
            "pipeline_name": self.dataset.dataset_name,
            "run_key": refresh_data.request_id,
            "metadata": metadata,
            "external_url": self.dataset.web_url,
        }
        if self.status.finished:
            LOGGER.debug("Refresh %s finished", refresh_data.request_id)
            pipeline_event_data.update({"event_timestamp": refresh_data.end_time})

            if self.status in (Status.COMPLETED_WITH_WARNINGS, Status.FAILED):
                error_code = json.loads(refresh_data.exception_json)["errorCode"] if refresh_data.exception_json else ""
                event_data = pipeline_event_data.copy()
                event_data.update({"task_key": refresh_data.request_id, "task_name": self.dataset.dataset_name})
                await send_message_log_event(
                    # cast handles a mypy bug; see https://github.com/python/mypy/issues/16659
                    cast(MemorySendChannel[Any], self.outbound_channel),
                    STATUS_TO_LOG_LEVEL[self.status],
                    ERROR_MESSAGE_DICT.get("errorCode", str(error_code)),
                    event_data,
                )

            # Close run task
            task_event_data = pipeline_event_data.copy()
            task_event_data.update({"task_key": refresh_data.request_id, "task_name": self.dataset.dataset_name})
            await send_run_status_event(
                # cast handles a mypy bug; see https://github.com/python/mypy/issues/16659
                cast(MemorySendChannel[Any], self.outbound_channel),
                self.status,
                task_event_data,
            )

            # Check affected reports if the dataset refreshed successfully
            if self.status == Status.COMPLETED:
                reports = await self.get_reports()
                for report in reports:
                    if report.dataset_id == self.dataset.dataset_id:
                        dataset_event_data = {
                            "event_timestamp": refresh_data.end_time,
                            "dataset_key": report.report_id,
                            "dataset_name": report.report_name,
                            "metadata": {"reportType": report.report_type, "dataset": vars(self.dataset)},
                            "external_url": report.web_url,
                        }
                        await send_dataset_operation_event(
                            # cast handles a mypy bug; see https://github.com/python/mypy/issues/16659
                            cast(MemorySendChannel[Any], self.outbound_channel),
                            "WRITE",
                            dataset_event_data,
                        )

            # Close run
            await send_run_status_event(
                # cast handles a mypy bug; see https://github.com/python/mypy/issues/16659
                cast(MemorySendChannel[Any], self.outbound_channel),
                self.status,
                pipeline_event_data,
            )
            self.finished_refreshes.append(refresh_data.request_id)
            self.status = Status.UNKNOWN
        else:
            LOGGER.debug("Refresh %s started", refresh_data.request_id)
            # Send run task status event
            task_event_data = pipeline_event_data.copy()
            task_event_data.update({"task_key": refresh_data.request_id, "task_name": self.dataset.dataset_name})
            await send_run_status_event(
                # cast handles a mypy bug; see https://github.com/python/mypy/issues/16659
                cast(MemorySendChannel[Any], self.outbound_channel),
                self.status,
                task_event_data,
            )
            # Send run status event
            await send_run_status_event(
                # cast handles a mypy bug; see https://github.com/python/mypy/issues/16659
                cast(MemorySendChannel[Any], self.outbound_channel),
                self.status,
                pipeline_event_data,
            )

    async def get_reports(self) -> list[PowerBIReport]:
        LOGGER.debug("Getting reports that are affected by `%s` dataset", self.dataset.dataset_name)
        response = await self.reports_endpoint.handle(
            path_args={"groupId": self.group_id},
        )
        if response.status_code == HTTPStatus.OK:
            data = response.json()
            if len(data["value"]) == 0:
                LOGGER.debug("No report uses dataset %s.", self.dataset.dataset_name)
            return [
                PowerBIReport(
                    report_id=d["id"],
                    report_name=d["name"],
                    report_type=d["reportType"],
                    dataset_id=self.dataset.dataset_id,
                    web_url=d["webUrl"],
                )
                for d in data["value"]
                if d["datasetId"] == self.dataset.dataset_id
            ]
        else:
            raise GenericApiError(f"Failed to fetch reports for {self.dataset.dataset_name} dataset.")

    async def get_refresh_history(self) -> dict | None:
        response = await self.endpoint.handle(
            query_params={"$top": "1"},
            path_args={"groupId": self.group_id, "datasetId": self.dataset.dataset_id},
        )
        if response.status_code == HTTPStatus.OK:
            data = response.json()
            return next(iter(data["value"]), None)
        else:
            raise GenericApiError(f"Failed to fetch dataset refresh history for {self.dataset.dataset_name} dataset.")

    async def execute(self, current_dt: datetime, previous_dt: datetime) -> None:
        refresh_data = await self.get_refresh_history()

        if not refresh_data:
            LOGGER.debug("No refresh data found for dataset %s.", self.dataset.dataset_name)
            return

        dataset_refresh = PowerBIDatasetRefresh(
            request_id=refresh_data["requestId"],
            refresh_type=refresh_data["refreshType"],
            status=refresh_data["status"],
            start_time=refresh_data["startTime"],
            end_time=refresh_data.get("endTime", None),
            exception_json=refresh_data.get("serviceExceptionJson", None),
        )
        # we don't expect to see status change to finished refresh.
        if dataset_refresh.request_id in self.finished_refreshes:
            LOGGER.debug("Refresh already seen. Continue.")
            return

        # Process active dataset refresh, including refresh that finishes during the session only.
        # This is to prevent sending duplicate past events, i.e. without this condition,
        # if the dataset has no new refresh, the agent will always send an event of the
        # last refresh whenever the agent restarts.
        if dataset_refresh.end_time is None or (
            datetime.fromisoformat(dataset_refresh.start_time).astimezone(UTC)
            >= self.start_time
            <= datetime.fromisoformat(dataset_refresh.end_time).astimezone(UTC)
        ):
            prev_status = self.status
            self.status = get_status(dataset_refresh.status)

            LOGGER.debug("%s dataset status: %s -> %s", self.dataset.dataset_name, prev_status, self.status)

            if prev_status != self.status:
                await self.report_status(dataset_refresh)
            else:
                LOGGER.debug("%s dataset: No status changed. Continue.", self.dataset.dataset_name)
        else:
            LOGGER.debug(
                "No active refresh found between %s - %s for %s dataset. Last refresh is from %s to %s.",
                previous_dt,
                current_dt,
                self.dataset.dataset_name,
                dataset_refresh.start_time,
                dataset_refresh.end_time,
            )
