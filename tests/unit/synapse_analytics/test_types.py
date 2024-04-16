import pytest
from azure.synapse.artifacts.models import (
    ActivityRun,
)

from agents.synapse_analytics.types import (
    SynapseActivityData,
)


@pytest.fixture()
def minimal_activity_run():
    activity_run = ActivityRun()
    activity_run.activity_name = "a name"
    activity_run.activity_type = "a type"
    activity_run.activity_run_id = "a run id"
    activity_run.pipeline_name = "a pipeline name"
    activity_run.pipeline_run_id = "a pipeline id"
    return activity_run


@pytest.mark.unit()
def test_create_synapse_activity_data_ok(minimal_activity_run):
    data = SynapseActivityData.create(minimal_activity_run)
    assert data is not None


@pytest.mark.unit()
@pytest.mark.parametrize(
    "essential_attribute",
    [
        "activity_name",
        "activity_type",
        "activity_run_id",
        "pipeline_name",
        "pipeline_run_id",
    ],
)
def test_create_synapse_activity_data_not_ok(minimal_activity_run, essential_attribute):
    setattr(minimal_activity_run, essential_attribute, None)
    with pytest.raises(ValueError, match="Invalid"):
        SynapseActivityData.create(minimal_activity_run)
