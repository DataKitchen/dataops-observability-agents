from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from azure.synapse.artifacts.models import ActivityRun

# These classes are created to make typing easier. The types returned by the API calls are all None even though some of
# the fields never actually are.


@dataclass(kw_only=True)
class SynapseActivityData:
    pipeline_run_id: str
    pipeline_name: str
    activity_name: str
    activity_type: str
    activity_run_id: str
    activity_run_start: datetime | None = None
    activity_run_end: datetime | None = None
    status: str | None = None
    input_: dict | None = None
    output: dict | None = None
    error: dict | None = None
    additional_properties: dict | None = None

    @classmethod
    def create(cls, activity: ActivityRun) -> SynapseActivityData:
        kwargs = {}
        for attr in [
            "activity_name",
            "activity_type",
            "activity_run_id",
            "pipeline_name",
            "pipeline_run_id",
        ]:
            if (value := getattr(activity, attr)) is None:
                raise ValueError(f"Invalid activity run field: {attr}")
            kwargs[attr] = value
        return SynapseActivityData(
            activity_run_start=activity.activity_run_start,
            activity_run_end=activity.activity_run_end,
            status=activity.status,
            input_=activity.input,
            output=activity.output,
            error=activity.error,
            additional_properties=activity.additional_properties,
            **kwargs,
        )


@dataclass(kw_only=True)
class SynapseRunData:
    pipeline_name: str
    run_id: str
