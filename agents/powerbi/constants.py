from framework.observability.message_event_log_level import MessageEventLogLevel
from toolkit.observability import Status

COMPONENT_TOOL: str = "power_bi"
"""The PowerBI tool type identifier string for Events API"""

STATUS_TO_LOG_LEVEL: dict[Status, str] = {
    Status.COMPLETED_WITH_WARNINGS: MessageEventLogLevel.WARNING.value,
    Status.FAILED: MessageEventLogLevel.ERROR.value,
}

ERROR_MESSAGE_DICT: dict[str, str] = {
    "DMTS_MonikerWithUnboundDataSources": "Data source is not bound to any connection.",
    "DMTS_UserNotFoundinADGraphError": "Azure Active Directory (Entra ID) user cannot be found.",
    "Gateway_Offline": "Gateway is offline.",
    "ModelRefresh_ShortMessage_CancelledByUser": "Dataset refresh was cancelled by the user.",
    "ModelRefresh_ShortMessage_ServiceError": "Power BI Service error.",
    "ModelRefresh_ShortMessage_CredentialsNotSpecified": "No credentials specified for the data source.",
    "ModelRefreshDisabled_CredentialNotSpecified": "No credentials specified for the data source.",
    "ProcessingTimeOut": "Processing of the data source timed out.",
}

POWERBI_DEFAULT_SCOPE: str = "https://analysis.windows.net/powerbi/api/.default"
"""The default scope to all PowerBI resource services (API)"""
