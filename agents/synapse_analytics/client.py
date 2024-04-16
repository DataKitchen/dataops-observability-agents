from azure.core.pipeline.policies import AsyncBearerTokenCredentialPolicy
from azure.core.pipeline.transport import TrioRequestsTransport
from azure.identity.aio import ClientSecretCredential
from azure.synapse.artifacts.aio import ArtifactsClient

from framework.configuration.authentication import (
    AzureServicePrincipalConfiguration,
)
from registry import ConfigurationRegistry

from .config import SynapseAnalyticsConfiguration


class _ClientWrapper:
    _client: ArtifactsClient | None = None

    async def __aenter__(self) -> ArtifactsClient:
        config = ConfigurationRegistry().lookup("synapse_analytics", SynapseAnalyticsConfiguration)
        auth_config = ConfigurationRegistry().lookup("auth_azure_spn", AzureServicePrincipalConfiguration)
        credential = ClientSecretCredential(
            client_id=auth_config.client_id,
            client_secret=auth_config.client_secret.get_secret_value(),
            tenant_id=auth_config.tenant_id,
            transport=TrioRequestsTransport(),
        )
        client = ArtifactsClient(
            credential=credential,
            authentication_policy=AsyncBearerTokenCredentialPolicy(
                credential,
                "https://dev.azuresynapse.net/.default",
            ),
            endpoint=config.client_endpoint,
            transport=TrioRequestsTransport(),
        )
        self._client = await client.__aenter__()
        return self._client

    async def __aexit__(self, *args: object) -> None:
        if self._client:
            await self._client.__aexit__(*args)
            self._client = None

    def __call__(self) -> ArtifactsClient:
        if not self._client:
            raise ValueError("Azure client has not been initialized")
        return self._client


artifacts_client: _ClientWrapper = _ClientWrapper()
