from datetime import UTC, datetime


class StateStore:
    _latest_event_timestamp: datetime | None

    def __init__(self) -> None:
        self._latest_event_timestamp = None

    @property
    def latest_event_timestamp(self) -> datetime | None:
        return self._latest_event_timestamp

    @latest_event_timestamp.setter
    def latest_event_timestamp(self, new_timestamp: datetime) -> None:
        """
        Store the timestamp of when the latest event was sent

        This function is not thread safe. It is async safe since it is a synchronous function.
        """
        utc_dt = new_timestamp.replace(tzinfo=UTC) if new_timestamp.tzinfo is None else new_timestamp.astimezone(UTC)
        if not self._latest_event_timestamp or self._latest_event_timestamp < utc_dt:
            self._latest_event_timestamp = utc_dt
