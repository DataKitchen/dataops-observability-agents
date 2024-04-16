from functools import cached_property
from typing import Self

from pydantic import Field, NonNegativeFloat, computed_field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

BASE_CLICK_BACK_URL: str = "https://web.azuresynapse.net/en/monitoring/pipelineruns"
SYNAPSE_WORKSPACE_ID_TEMPLATE: str = (
    "/subscriptions/{subscription}/resourceGroups/{resource_group}/providers/Microsoft.Synapse/workspaces/{workspace}"
)
CLIENT_ENDPOINT: str = "https://{workspace_name}.dev.azuresynapse.net"


class SynapseAnalyticsConfiguration(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="DK_SYNAPSE_ANALYTICS_")
    period: NonNegativeFloat = 5.0
    workspace_name: str
    subscription_id: str | None = None
    """
    Subscription id

    Only required to generate links back to Synapse
    """
    resource_group_name: str | None = None
    """
    The resource group that the Synapse instance resides in

    Only required to generate links back to Synapse
    """
    pipelines_filter: list[str] = Field(default_factory=list)
    """
    Optional list of exact names of pipelines to monitor
    """

    @computed_field  # type: ignore[misc]
    @cached_property
    def client_endpoint(self) -> str:
        if workspace_name := self.workspace_name:
            return CLIENT_ENDPOINT.format(workspace_name=workspace_name)
        return ""

    @computed_field  # type: ignore[misc]
    @cached_property
    def workspace_id(self) -> str | None:
        if (subscription := self.subscription_id) and (resource_group := self.resource_group_name):
            return SYNAPSE_WORKSPACE_ID_TEMPLATE.format(
                subscription=subscription,
                resource_group=resource_group,
                workspace=self.workspace_name,
            )
        return None

    @computed_field  # type: ignore[misc]
    @property
    def base_click_back_url(self) -> str | None:
        return BASE_CLICK_BACK_URL

    @model_validator(mode="after")
    def check_click_back_url_fields(self) -> Self:
        if bool(self.subscription_id) != bool(self.resource_group_name):
            raise ValueError("Set both subscription_id and resource_group_name to generate links back to Synapse")
        return self
