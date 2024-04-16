import struct
from collections.abc import AsyncIterator, Iterable
from datetime import datetime, timedelta, timezone
from typing import Any

from trio import to_thread

from .config import SsisConfiguration

DB_DEFAULT_BATCH_SIZE = 500


class AsyncConn:
    def __init__(self, configuration: SsisConfiguration):
        self.config = configuration

    async def open_connection(self) -> Any:
        # We are importing `pyodbc` here to force it to happen only when the agent type is SSIS. This is helpful to
        # avoid errors on environments that lacks UNIXODBC support, which is likely to happen at local envs
        import pyodbc  # type: ignore[import-not-found] # noqa: PLC0415

        conn_str = (
            f"DRIVER={self.config.db_driver};"
            f"SERVER={self.config.db_host},{self.config.db_port};"
            f"DATABASE={self.config.db_name};"
            f"UID={self.config.db_user};"
            f"PWD={self.config.db_password.get_secret_value()};"
        )
        conn = await to_thread.run_sync(pyodbc.connect, conn_str)

        # The following code is heavily based on how SQLAlchemy handles datetime columns
        # https://github.com/sqlalchemy/sqlalchemy/blob/900d13acb4f19de955eb609dea52a755f0d11acb/lib/sqlalchemy/dialects/mssql/pyodbc.py#L700

        def _handle_datetime(dto_value: bytes) -> datetime:
            """Converts a bytes object result from a timestamp column into a python datetime object."""
            tup = struct.unpack("<6hI2h", dto_value)
            tzinfo = timezone(timedelta(hours=tup[7], minutes=tup[8]))
            return datetime(tup[0], tup[1], tup[2], tup[3], tup[4], tup[5], tup[6] // 1000, tzinfo=tzinfo)

        conn.add_output_converter(-155, _handle_datetime)

        return conn

    async def exec_and_fetch_all(
        self,
        query: str,
        params: Iterable[Any] = (),
        batch_size: int = DB_DEFAULT_BATCH_SIZE,
        model: type | None = None,
    ) -> AsyncIterator:
        # FIXME It's not optimal to open a different connection for each query, but other strategies were not working
        #       as expected.
        conn = await self.open_connection()
        cursor = await to_thread.run_sync(conn.execute, query, params)
        with cursor:
            columns = [d[0] for d in cursor.description]
            while batch := await to_thread.run_sync(cursor.fetchmany, batch_size):
                for row in batch:
                    yield model(**dict(zip(columns, row, strict=True))) if model else row
