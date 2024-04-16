import pytest

from agents.synapse_analytics.helpers import get_observability_status
from toolkit.observability import Status


@pytest.mark.unit()
@pytest.mark.parametrize(
    ("synapse_status", "observability_status"),
    [
        ("InProgress", Status.RUNNING),
        ("Succeeded", Status.COMPLETED),
        ("Uncertain", Status.COMPLETED_WITH_WARNINGS),
        ("Failed", Status.FAILED),
        ("Cancelled", Status.FAILED),
        ("Canceled", Status.FAILED),
        ("Queued", Status.UNKNOWN),
        ("Cancelling", Status.UNKNOWN),
        ("Canceling", Status.UNKNOWN),
        ("INVALID STATUS", Status.UNKNOWN),
        (None, Status.UNKNOWN),
    ],
)
def test_create_synapse_activity_data_not_ok(synapse_status, observability_status):
    assert get_observability_status(synapse_status) == observability_status
