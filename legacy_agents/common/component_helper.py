import json
import logging
import os
from datetime import datetime
from logging import Logger
from typing import Any, Optional, Union
from urllib.parse import quote

import requests
from attrs import define, field, validators

logger: Logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

VERIFY_SSL = {"true": True, "false": False}[os.getenv("DK_EVENTS_VERIFY_SSL", "true").lower()]

if not VERIFY_SSL:
    import urllib3
    from urllib3 import exceptions

    urllib3.disable_warnings(exceptions.InsecureRequestWarning)


@define(kw_only=True, slots=False)
class ComponentHelper:
    key: str = field(validator=validators.instance_of(str))
    name: str = field(validator=validators.instance_of(str))
    tool: str = field(validator=validators.instance_of(str))
    description: str = field(validator=validators.instance_of(str))
    # schedule: Optional[str] = None

    _component_id: Optional[str] = None

    def _check_connection(self) -> None:
        err_message = ""
        required_envvars = ["EVENTS_PROJECT_ID", "EVENTS_API_KEY", "EVENTS_API_HOST"]
        for required_envvar in required_envvars:
            if required_envvar not in os.environ:
                err_message = (
                    err_message + f"{required_envvar} is not set in the environment variables.\n"
                )

        if len(err_message) != 0:
            logger.info(err_message)
            raise ValueError(err_message)

        self.project_id = os.environ["EVENTS_PROJECT_ID"]
        self.api_key = os.environ["EVENTS_API_KEY"]
        self.api_host = os.environ["EVENTS_API_HOST"]

    def delete_component(self) -> bool:
        self._check_connection()
        url = f"{self.api_host}/observability/v1/components/{quote(self.key)}"
        headers = {"Accept": "application/json", "ServiceAccountAuthenticationKey": self.api_key}
        try:
            response = requests.delete(url, headers=headers, verify=VERIFY_SSL)
        except requests.exceptions.RequestException as e:
            logger.info(e)
            raise Exception(e)

        if response.status_code == 204 or response.status_code == 404:
            return True
        else:
            logger.info(f"{response.status_code} - {response.reason} - {response.text}")
            raise Exception(f"{response.status_code} - {response.reason} - {response.text}")

    def find_component(self) -> dict:
        """Find a component by key and return the component object

        Returns:
            dict: The component metadata. If the component is not found, returns an empty dict.
        """
        self._check_connection()
        url = f"{self.api_host}/observability/v1/projects/{self.project_id}/components?search={quote(self.key)}"
        headers = {"Accept": "application/json", "ServiceAccountAuthenticationKey": self.api_key}
        try:
            response = requests.get(url, headers=headers, verify=VERIFY_SSL)
        except requests.exceptions.RequestException as e:
            logger.error(e)
            raise Exception(e)

        if response.status_code == 200:
            response_json = json.loads(response.text)
            if "entities" in response_json:
                if len(response_json["entities"]) == 1:
                    self._component_id = response_json["entities"][0]["id"]
                    if isinstance(response_json["entities"][0], dict):
                        return response_json["entities"][0]
                    else:
                        raise Exception(
                            f"Found {len(response_json['entities'])} components with key {self.key}"
                        )
                elif len(response_json["entities"]) > 1:
                    # This is unexpected, and we can't recover
                    logger.error(
                        f"Found {len(response_json['entities'])} components with key {self.key}"
                    )
                    raise Exception(
                        f"Found {len(response_json['entities'])} components with key {self.key}"
                    )
                else:
                    # This is a reasonable response. It means that we couldn't find the component.
                    return {}
        else:
            logger.error(
                f"Error finding component: {response.status_code} - {response.reason} - {response.text}"
            )
            raise Exception(
                f"Error finding component: {response.status_code} - {response.reason} - {response.text}"
            )
        return {}

    def set_schedule(self, interval_minutes: int, grace_period_seconds: int) -> dict:
        if self._component_id is None:
            raise Exception("Component ID is not set")

        self._check_connection()
        url = f"{self.api_host}/observability/v1/components/{quote(self._component_id)}/schedules"
        headers = {"Accept": "application/json", "ServiceAccountAuthenticationKey": self.api_key}

        # Timezone defaults to UTC
        payload = {
            "margin": grace_period_seconds,
            "description": "How often do I checkin?",
            "expectation": "BATCH_PIPELINE_START_TIME",
            "schedule": f"*/{interval_minutes} * * * *",
        }

        try:
            response = requests.post(url, headers=headers, json=payload, verify=VERIFY_SSL)
        except requests.exceptions.RequestException as e:
            logger.info(e)
            raise ValueError(e)

        if response.status_code == 201:
            response_json = json.loads(response.text)
            # Verify the information
            return dict(response_json)
        else:
            raise Exception(
                f"Error creating schedule: {response.status_code} - {response.reason} - {response.text}"
            )

    def create_component(self) -> Any:
        self._check_connection()
        url = f"{self.api_host}/observability/v1/projects/{self.project_id}/batch-pipelines"
        headers = {"Accept": "application/json", "ServiceAccountAuthenticationKey": self.api_key}

        payload = {
            "name": self.name,
            "description": self.description,
            "key": self.key,
            "type": "BATCH_PIPELINE",
            "tool": self.tool,
        }

        try:
            response = requests.post(url, headers=headers, json=payload, verify=VERIFY_SSL)
        except requests.exceptions.RequestException as e:
            logger.info(e)
            raise ValueError(e)

        if response.status_code == 201:
            response_json = json.loads(response.text)
            # Verify the information
            return response_json
        else:
            raise Exception(
                f"Error creating component: {response.status_code} - {response.reason} - {response.text}"
            )

    def create_component_if_not_exists(self) -> dict:
        """See if the component exists. If not, create it"""
        self._check_connection()
        component_metadata = self.find_component()
        if len(component_metadata) == 0:
            component_metadata = self.create_component()
            if len(component_metadata) == 0:
                raise Exception(f"Unable to create Component '{self.name}'")
            else:
                self._component_id = component_metadata["id"]
        return component_metadata

    def get_label(self, key: str) -> Optional[str]:
        """The last sent event is stored in the Observability API. The stored timestamp is in UTC."""
        self._check_connection()
        # component_key = 'upset_accept_air_10_10_38'
        component_metadata = self.create_component_if_not_exists()
        if len(component_metadata) == 0:
            raise Exception(f"Unable to create Component '{self.name}'")

        if "labels" in component_metadata and component_metadata["labels"] is not None:
            if key in component_metadata["labels"]:
                return str(component_metadata["labels"][key])
        return None

    def set_label(self, key: str, value: str) -> dict:
        self._check_connection()
        component_metadata = self.create_component_if_not_exists()
        if len(component_metadata) == 0:
            raise Exception(f"Unable to create Component '{self.name}'")

        url = f"{self.api_host}/observability/v1/batch-pipelines/{component_metadata['id']}"
        headers = {"Accept": "application/json", "ServiceAccountAuthenticationKey": self.api_key}

        component_metadata_update = dict(
            labels={key: value},
        )

        if component_metadata["labels"] is not None:
            updated_labels = component_metadata["labels"]
            updated_labels.update(component_metadata_update["labels"])
            component_metadata_update["labels"] = updated_labels

        try:
            response = requests.patch(
                url, headers=headers, json=component_metadata_update, verify=VERIFY_SSL
            )
        except requests.exceptions.RequestException as e:
            logger.info(e)
            raise ValueError(e)

        if response.status_code == 200:
            response_json = json.loads(response.text)
            # Verify the information
            return dict(response_json)
        else:
            raise Exception(
                f"Error updating component label: {response.status_code} - {response.reason} - {response.text}"
            )
