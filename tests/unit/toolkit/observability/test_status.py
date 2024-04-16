import pytest

from toolkit.observability import Status


@pytest.mark.unit()
@pytest.mark.parametrize(
    argnames="status",
    argvalues=[Status.COMPLETED, Status.RUNNING, Status.COMPLETED_WITH_WARNINGS, Status.UNKNOWN],
)
def test_status_finished(status):
    if status in (Status.COMPLETED, Status.COMPLETED_WITH_WARNINGS, Status.FAILED):
        assert status.finished
    else:
        assert not status.finished
