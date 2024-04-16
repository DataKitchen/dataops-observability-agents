from datetime import UTC, datetime, timedelta

import pytest

from framework.states import StateStore


@pytest.mark.unit()
def test_store_latest_event_timestamp():
    now = datetime.now()  # noqa: DTZ005
    tz_now = now.replace(tzinfo=UTC)
    earlier = now - timedelta(minutes=10)

    state_store = StateStore()
    assert state_store.latest_event_timestamp is None

    state_store.latest_event_timestamp = now
    assert state_store.latest_event_timestamp == tz_now
    assert state_store.latest_event_timestamp.tzinfo == UTC

    state_store.latest_event_timestamp = earlier
    assert state_store.latest_event_timestamp == tz_now
