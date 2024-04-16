from datetime import datetime

import pytest

from framework.core.tasks import PeriodicTask


# because PeriodicTask has an abstract method, we need a dummy implementation
class TestPeriodicTask(PeriodicTask):
    async def execute(self, current_dt: datetime, previous_dt: datetime) -> None:
        pass


@pytest.mark.unit()
async def test_periodic_task_finish(mock_core_env_vars):
    task = TestPeriodicTask()
    assert not task.is_done
    task.finish()
    assert task.is_done
