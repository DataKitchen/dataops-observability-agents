import logging
from datetime import UTC, datetime
from types import TracebackType
from typing import Any, Self, cast

from httpx import URL
from pydantic import HttpUrl
from trio import MemorySendChannel, Nursery

from framework.authenticators import TokenAuth
from framework.configuration.http import HTTPClientConfig
from framework.core.handles import get_client
from framework.core.loops import PeriodicLoop
from framework.core.tasks import PeriodicTask
from framework.observability.message_event_log_level import MessageEventLogLevel
from registry import ConfigurationRegistry
from toolkit.more_typing import JSON_DICT
from toolkit.observability import EventType, Status

from .configuration import QlikConfiguration
from .constants import COMPONENT_TOOL
from .handles import QlikGetAppsEndpoint, QlikGetReloadsEndpoint
from .helpers import get_status

LOGGER = logging.getLogger(__name__)
DATETIME_FORMAT: str = "%Y-%m-%dT%H:%M:%S.%fZ"


def str_to_utc_datetime(datetime_str: str) -> datetime:
    return datetime.strptime(datetime_str, DATETIME_FORMAT).replace(tzinfo=UTC)


def utc_datetime_to_isoformat(datetime_str: str) -> str:
    return datetime.strptime(datetime_str, DATETIME_FORMAT).replace(tzinfo=UTC).isoformat()


class QlikListReloadsTask(PeriodicTask):
    def __init__(
        self,
        nursery: Nursery,
        outbound_channel: MemorySendChannel[JSON_DICT],
    ):
        super().__init__(outbound_channel=outbound_channel)
        self.nursery = nursery
        self.registry = ConfigurationRegistry()
        self.configuration = self.registry.lookup("qlik", QlikConfiguration)
        self.configuration.base_api_url = HttpUrl(
            str(self.configuration.base_api_url).replace(
                "tenant",
                self.configuration.tenant,
            ),
        )
        self.token = self.configuration.api_key
        self.http_config = self.registry.mutate("http", HTTPClientConfig, auth=TokenAuth(self.token))
        self.client = get_client(self.http_config)
        self.get_apps_endpoint = QlikGetAppsEndpoint(
            base_url=URL(str(self.configuration.base_api_url)),
            client=self.client,
        )
        self.get_reload_endpoint = QlikGetReloadsEndpoint(
            base_url=URL(str(self.configuration.base_api_url)),
            client=self.client,
        )
        self.apps_filter = self.configuration.apps
        self.runs_watched: dict[str, QlikWatchReloadTask] = {}

    async def __aenter__(self) -> Self:
        await self.client.__aenter__()
        return await super().__aenter__()

    async def __aexit__(
        self,
        exc_type: type[BaseException],
        exc_val: BaseException,
        exc_tb: TracebackType,
    ) -> None:
        await self.client.__aexit__(exc_type, exc_val, exc_tb)
        return await super().__aexit__(exc_type, exc_val, exc_tb)

    async def get_apps(self) -> list[Any]:
        response = await self.get_apps_endpoint.handle()
        if response.status_code == 200:
            response_json = response.json()
            if "data" in response_json:
                apps = list(response.json()["data"])
                return apps
            else:
                return []
        else:
            LOGGER.error(f"Error getting apps: {response.status_code} - {response.text}")
            return []

    async def get_reload(self, app_id: str) -> Any:
        response = await self.get_reload_endpoint.handle(query_params={"appId": app_id, "limit": 1})
        if response.status_code == 200:
            response_json = response.json()
            if "data" in response_json:
                result = response_json["data"]
                if len(result) > 0:
                    return result[0]
                return None
            else:
                return None

    async def execute(self, current_dt: datetime, previous_dt: datetime) -> None:
        all_apps = await self.get_apps()
        new_runs = []
        if self.apps_filter:
            apps = [
                app
                for app in all_apps
                if app["attributes"]["name"] in self.apps_filter and app["attributes"]["id"] not in self.runs_watched
            ]
        else:
            apps = all_apps

        for app in apps:
            if app_details := app.get("attributes"):
                app_id = app_details.get("id")
                reload = await self.get_reload(app_id=app_id)
                if reload and reload.get("startTime"):
                    reload_starttime = str_to_utc_datetime(reload.get("startTime"))
                    if previous_dt <= reload_starttime <= current_dt:
                        if reload.get("id") not in self.runs_watched:
                            new_runs.append(
                                {
                                    "app_id": app_details.get("id"),
                                    "app_name": app_details.get("name"),
                                    "reload_id": reload.get("id"),
                                    "app_details": {
                                        "app_description": app_details.get("description"),
                                        "app_created_date": app_details.get("createdDate"),
                                        "app_modified_date": app_details.get("modifiedDate"),
                                        "app_owner": app_details.get("owner"),
                                        "app_owner_id": app_details.get("ownerId"),
                                        "app_published": app_details.get("published"),
                                        "app_published_time": app_details.get("publishTime"),
                                    },
                                },
                            )
        for run in new_runs:
            get_run_task = QlikWatchReloadTask(
                run=run,
                configuration=self.configuration,
                http_config=self.http_config,
                outbound_channel=cast(MemorySendChannel[JSON_DICT], self.outbound_channel.clone()),
            )
            self.nursery.start_soon(PeriodicLoop(period=self.configuration.period, task=get_run_task).run)
            self.runs_watched[cast(str, run["reload_id"])] = get_run_task
            LOGGER.debug(f"Log Run to be observed: {run['reload_id']}")

        runs_finished: list[str] = []
        for key, run_task in self.runs_watched.items():
            if run_task.is_done:
                runs_finished.append(key)
        for finished_run in runs_finished:
            self.runs_watched.pop(finished_run)


class QlikWatchReloadTask(PeriodicTask):
    def __init__(
        self,
        run: JSON_DICT,
        configuration: QlikConfiguration,
        http_config: HTTPClientConfig,
        outbound_channel: MemorySendChannel[JSON_DICT],
    ) -> None:
        super().__init__(outbound_channel=outbound_channel)
        self.configuration = configuration
        self.http_config = http_config
        self.client = get_client(self.http_config)
        self.get_reload_endpoint = QlikGetReloadsEndpoint(
            base_url=URL(str(self.configuration.base_api_url)),
            client=self.client,
        )
        self.run = run
        self.new_run = True
        self.pipeline_name = str(self.run.get("app_id"))
        self.pipeline_key = str(self.run.get("app_name"))
        self.run_key = str(self.run.get("reload_id"))
        self.status = Status.UNKNOWN

    async def get_reload(self, app_id: str) -> Any:
        response = await self.get_reload_endpoint.handle(query_params={"appId": app_id, "limit": 1})
        if response.status_code == 200:
            response_json = response.json()
            if "data" in response_json:
                result = response_json["data"]
                if len(result) > 0:
                    return result[0]
                return None
            else:
                return None

    async def execute(self, current_dt: datetime, previous_dt: datetime) -> None:
        self.reload = await self.get_reload(app_id=str(self.run.get("app_id")))
        if self.reload:
            prev_status = self.status
            self.status = get_status(self.reload.get("status"))

            event_starttime = str(utc_datetime_to_isoformat(str(self.reload.get("startTime"))))
            task_key = self.pipeline_name
            task_name = self.pipeline_key
            metadata: JSON_DICT = {
                "reload_id": self.reload.get("id"),
                "app_id": self.reload.get("appId"),
                "tenant_id": self.reload.get("tenantId"),
                "user_id": self.reload.get("userId"),
                "reload_type": self.reload.get("type"),
                "partial": self.reload.get("partial"),
                "creationTime": self.reload.get("creationTime"),
                "engineTime": self.reload.get("engineTime"),
                "app_details": self.run.get("app_details"),
            }
            payload: JSON_DICT = {
                "EVENT_TYPE": EventType.RUN_STATUS.value,
                "status": self.status.value,
                "metadata": metadata,
                "pipeline_key": self.pipeline_key,
                "run_key": self.run_key,
                "external_url": self.reload["links"]["self"]["href"],
                "component_tool": COMPONENT_TOOL,
            }
            run_event = payload.copy()
            task_event = payload.copy()
            task_event.update({"task_key": task_key, "task_name": task_name})

            if self.new_run:
                run_event.update(
                    {
                        "EVENT_TYPE": EventType.RUN_STATUS.value,
                        "event_timestamp": event_starttime,
                        "status": Status.RUNNING.value,
                    },
                )
                task_event.update(
                    {
                        "EVENT_TYPE": EventType.RUN_STATUS.value,
                        "event_timestamp": event_starttime,
                        "status": Status.RUNNING.value,
                    },
                )
                self.new_run = False
                await self.send(task_event)
                await self.send(run_event)

            if prev_status != self.status:
                if self.status.finished:
                    run_end_event = payload.copy()
                    task_end_event = payload.copy()
                    event_endtime = str(utc_datetime_to_isoformat(str(self.reload.get("endTime"))))
                    LOGGER.info(f"Refresh {self.run_key} finished: {run_event}")
                    run_end_event.update(
                        {
                            "EVENT_TYPE": EventType.RUN_STATUS.value,
                            "event_timestamp": event_endtime,
                        },
                    )
                    task_end_event.update(
                        {
                            "EVENT_TYPE": EventType.RUN_STATUS.value,
                            "event_timestamp": event_endtime,
                            "task_key": task_key,
                            "task_name": task_name,
                        },
                    )
                    if self.status in (Status.COMPLETED_WITH_WARNINGS, Status.FAILED):
                        message_log_event = payload.copy()
                        del message_log_event["status"]
                        message_log_event.update({"task_key": task_key, "task_name": task_name})
                        message_log_event["message"] = self.reload.get("log")
                        message_log_event["event_timestamp"] = event_endtime
                        message_log_event["EVENT_TYPE"] = EventType.MESSAGE_LOG.value
                        message_log_event["log_level"] = MessageEventLogLevel.ERROR.value
                        message_log_event["message"] = self.reload.get("log")
                        LOGGER.info(f"Send MessageLogEvent: {message_log_event}")
                        await self.send(message_log_event)
                    await self.send(task_end_event)
                    await self.send(run_end_event)
