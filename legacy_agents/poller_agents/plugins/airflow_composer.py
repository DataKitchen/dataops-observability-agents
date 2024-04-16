from __future__ import annotations

import base64
import hashlib
import json
import logging
import os
from datetime import datetime
from typing import Any, Dict, List
from urllib.parse import urlencode

import requests
from attrs import define, field, validators
from dateutil import parser
from google.auth.transport.requests import Request
from google.oauth2 import service_account
from requests import Response

from common.events_publisher import EventsPublisher
from common.message_event_log_level import MessageEventLogLevel
from common.status import Status
from poller_agents.abstract_run import AbstractRun
from poller_agents.abstract_runs_fetcher import AbstractRunsFetcher

logger = logging.getLogger(__name__)

CLIENT_ID: str = os.getenv("COMPOSER_CLIENT_ID", "")
WEBSERVER_ID: str = os.getenv("COMPOSER_WEBSERVER_ID", "")

COMPOSER_2: bool = bool(os.getenv("COMPOSER_2_FLAG", False))
COMPOSER_2_WEB_URL: str = os.getenv("COMPOSER_2_WEB_URL", "")
AUTH_SCOPE: str = os.getenv("COMPOSER_2_AUTH_SCOPE", "")

BASE64_ENCODED_SERVICE_ACCOUNT_STR: str = os.getenv("BASE64_ENCODED_SERVICE_ACCOUNT_STR", "")

base64_encoded_bytes: bytes = BASE64_ENCODED_SERVICE_ACCOUNT_STR.encode("ascii")
base64_decoded_bytes: bytes = base64.b64decode(base64_encoded_bytes)
decoded_string: str = base64_decoded_bytes.decode("ascii")
service_account_dict: dict = json.loads(decoded_string, strict=False)

base_composer_api_url: str = (
    f"{COMPOSER_2_WEB_URL}/api/v1" if COMPOSER_2 else f"https://{WEBSERVER_ID}.appspot.com/api/v1"
)
base_click_back_url: str = (
    COMPOSER_2_WEB_URL if COMPOSER_2 else f"https://{WEBSERVER_ID}.appspot.com"
)
VERIFY_SSL: bool = {"true": True, "false": False}[os.getenv("TARGET_VERIFY_SSL", "true").lower()]


def get_status(record: dict) -> Status:
    # Airflow Dag Run States: "queued", "running", "success", "failed".
    status = record["state"]
    if status == "queued":
        return Status.UNKNOWN
    elif status == "running":
        return Status.RUNNING
    elif status == "success":
        return Status.COMPLETED
    elif status == "failed":
        return Status.FAILED
    else:
        logger.error(f"Unrecognized status: {status}. Setting status to {Status.UNKNOWN.name}")
        return Status.UNKNOWN


def get_id_token(**kwargs: Any) -> str:
    if "audience" in kwargs:
        credentials = service_account.IDTokenCredentials.from_service_account_info(
            info=service_account_dict, target_audience=kwargs["audience"]
        )
    elif "scopes" in kwargs:
        credentials = service_account.Credentials.from_service_account_info(
            info=service_account_dict, scopes=kwargs["scopes"]
        )
    request = Request()
    credentials.refresh(request)
    token: str = credentials.token if credentials.token else ""
    return token


def make_iap_request(url: str, method: str = "GET", **kwargs: Any) -> Response:
    if "timeout" not in kwargs:
        kwargs["timeout"] = 90
    if COMPOSER_2:
        google_open_id_connect_token = get_id_token(scopes=[AUTH_SCOPE])
    else:
        google_open_id_connect_token = get_id_token(audience=CLIENT_ID)
    response = requests.request(
        method,
        url,
        headers={"Authorization": "Bearer {}".format(google_open_id_connect_token)},
        verify=VERIFY_SSL,
        **kwargs,
    )
    if response.status_code == 403:
        raise Exception(
            "Service account does not have permission to access the IAP-protected application."
        )
    elif response.status_code != 200:
        raise Exception(
            "Bad response from application: {!r} / {!r} / {!r}".format(
                response.status_code, response.headers, response.text
            )
        )
    else:
        return response


@define(kw_only=True, slots=False)
class ComposerRunsFetcher(AbstractRunsFetcher):
    base_api_url: str = field(validator=validators.instance_of(str))

    _component_tool: str = "airflow"

    @property
    def agent_name(self) -> str:
        return f"{self.base_api_url}"

    @property
    def agent_key(self) -> str:
        md5 = hashlib.md5()
        md5.update(self.agent_name.encode("utf-8"))
        return md5.hexdigest()

    @property
    def component_tool(self) -> str:
        return self._component_tool

    @classmethod
    def create_runs_fetcher(cls, events_publisher: EventsPublisher) -> AbstractRunsFetcher:
        base_api_url = base_composer_api_url
        if base_api_url is None:
            raise ValueError(f"base_composer_api_url variable has not been set.")
        return ComposerRunsFetcher(events_publisher=events_publisher, base_api_url=base_api_url)

    def fetch_runs(
        self, execution_date_gte: datetime, execution_date_lte: datetime
    ) -> List[AbstractRun]:
        api_endpoint_url = os.path.join(self.base_api_url, "dags/~/dagRuns/list")

        body: Dict[str, Any] = {
            "dag_ids": self._get_dag_ids(),
            "execution_date_gte": execution_date_gte.astimezone().isoformat(),
            "execution_date_lte": execution_date_lte.astimezone().isoformat(),
        }
        response = make_iap_request(url=api_endpoint_url, method="POST", json=body).json()
        return [self._create_composer_run(r) for r in response["dag_runs"]]

    def _get_dag_ids(self) -> List[str]:
        api_endpoint_url = os.path.join(self.base_api_url, "dags")
        response = make_iap_request(url=api_endpoint_url, method="GET")
        return [d["dag_id"] for d in response.json()["dags"]]

    def _create_composer_run(self, dag_run: dict) -> ComposerRun:
        return ComposerRun(
            events_publisher=self.events_publisher,
            pipeline_key=dag_run["dag_id"],
            run_key=dag_run["dag_run_id"],
            base_api_url=self.base_api_url,
            agent_name=self.agent_name,
            agent_key=self.agent_key,
        )


@define(kw_only=True)
class ComposerRun(AbstractRun):
    base_api_url: str = field(validator=validators.instance_of(str))
    tasks: List[ComposerTask] = field(default=[])

    _component_tool = "airflow"

    @property
    def component_tool(self) -> str:
        return self._component_tool

    def update(self) -> None:
        logger.info(f"Updating {self.pipeline_key} run {self.run_key}...")
        api_endpoint_url = os.path.join(
            self.base_api_url, "dags", self.pipeline_key, "dagRuns", self.run_key
        )
        response = make_iap_request(url=api_endpoint_url, method="GET").json()

        self.update_tasks()
        status = get_status(response)

        if status.finished:
            self.finished = True
            logger.info(f"Composer Run {self.run_key} for Pipeline : {self.pipeline_key} finished.")
            event_timestamp = parser.parse(response["end_date"])
            execution_date_str = urlencode(
                query={"execution_date": response["execution_date"]}, doseq=True
            )
            click_back_url = (
                f"{base_click_back_url}/dags/{self.pipeline_key}/graph?{execution_date_str}"
            )
            metadata = {
                "pipeline_name": self.pipeline_key,
                "dag_run_id": self.run_key,
                "start_date": response["start_date"],
                "end_date": response["end_date"],
                "pipeline_status": response["state"],
            }
            self.publish_run_status_event(
                event_timestamp, None, status, external_url=click_back_url, metadata=metadata
            )

    def update_tasks(self) -> None:
        if len(self.tasks) == 0:
            self.tasks = [self.create_task(r) for r in self.get_task_records_dict().values()]
        else:
            task_records_dict = self.get_task_records_dict()
            for t in self.tasks:
                if not t.finished:
                    t.update(task_records_dict[t.name])

    def get_task_records_dict(self) -> dict:
        api_endpoint_url = os.path.join(
            self.base_api_url, "dags", self.pipeline_key, "dagRuns", self.run_key, "taskInstances"
        )
        response = make_iap_request(url=api_endpoint_url, method="GET").json()
        logger.info("task_record: \n" + str(response))
        return {t["task_id"]: t for t in response["task_instances"]}

    def create_task(self, record: dict) -> ComposerTask:
        composer_task = ComposerTask(composer_run=self, name=record["task_id"])
        composer_task.update(record)
        logger.info("create task record: \n" + str(composer_task))
        return composer_task


@define(kw_only=True)
class ComposerTask:
    composer_run: ComposerRun = field(validator=validators.instance_of(ComposerRun))
    name: str = field(validator=validators.instance_of(str))
    event_published: bool = field(default=False, validator=validators.instance_of(bool))
    status: Status = field(default=Status.UNKNOWN, validator=validators.instance_of(Status))

    @property
    def finished(self) -> bool:
        return self.status.finished

    def get_failed_task_message(
        self, dag_id: str, dag_run_id: str, task_id: str, task_try_number: int
    ) -> str:
        api_endpoint_url = os.path.join(
            self.composer_run.base_api_url,
            f"dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs/{task_try_number}",
        )
        response = make_iap_request(api_endpoint_url, method="GET").text
        err_msg_start_idx = response.find("ERROR - ")
        error_msg_txt = response[err_msg_start_idx:]
        err_msg_end_idx = error_msg_txt.find(".\n")
        error_msg: str = (
            error_msg_txt[:err_msg_end_idx]
            if error_msg_txt
            else "Please use click back link for detailed logs."
        )
        return error_msg

    def update(self, record: dict) -> None:
        prev_status = self.status
        self.status = get_status(record)

        logger.info(f"Current state of task {self.name}: {self.status.value}")
        params = {
            "dag_id": record["dag_id"],
            "task_id": record["task_id"],
            "execution_date": record["execution_date"],
        }
        query_str = urlencode(query=params, doseq=True)
        click_back_url = f"{base_click_back_url}/log?{query_str}"

        metadata_dict = {
            "hostname": record["hostname"],
            "try_number": record["try_number"],
            "operator": record["operator"],
        }

        if not self.event_published and self.status != Status.UNKNOWN:
            event_timestamp = parser.parse(record["start_date"])
            self.composer_run.publish_run_status_event(
                event_timestamp,
                self.name,
                Status.RUNNING,
                metadata=metadata_dict,
                external_url=click_back_url,
            )
            self.event_published = True

            if self.status.finished:
                event_timestamp = parser.parse(record["end_date"])
                self.composer_run.publish_run_status_event(
                    event_timestamp,
                    self.name,
                    self.status,
                    metadata=metadata_dict,
                    external_url=click_back_url,
                )
                if self.status == Status.FAILED:
                    task_failure_message = self.get_failed_task_message(
                        record["dag_id"],
                        record["dag_run_id"],
                        record["task_id"],
                        record["try_number"],
                    )
                    self.composer_run.publish_message_log_event(
                        event_timestamp=event_timestamp,
                        log_level=MessageEventLogLevel.ERROR,
                        message=task_failure_message,
                        task_key=self.name,
                        task_name=self.name,
                        metadata=metadata_dict,
                        external_url=click_back_url,
                    )

        elif prev_status != self.status and self.status != Status.UNKNOWN:
            event_timestamp = parser.parse(record["end_date"])
            if self.status == Status.FAILED:
                task_failure_message = self.get_failed_task_message(
                    record["dag_id"], record["dag_run_id"], record["task_id"], record["try_number"]
                )
                self.composer_run.publish_message_log_event(
                    event_timestamp=event_timestamp,
                    log_level=MessageEventLogLevel.ERROR,
                    message=task_failure_message,
                    task_key=self.name,
                    task_name=self.name,
                    metadata=metadata_dict,
                    external_url=click_back_url,
                )
            self.composer_run.publish_run_status_event(
                event_timestamp=event_timestamp,
                task_key=self.name,
                status=self.status,
                metadata=metadata_dict,
                external_url=click_back_url,
            )
