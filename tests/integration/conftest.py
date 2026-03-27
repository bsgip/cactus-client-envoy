import os
from datetime import datetime, timezone
from typing import Generator

import pytest
from assertical.fixtures.environment import environment_snapshot
from assertical.fixtures.postgres import generate_async_conn_str_from_connection
from psycopg import Connection

from envoy.server.alembic import upgrade
from envoy.server.model.aggregator import NULL_AGGREGATOR_ID


@pytest.fixture
def pg_empty_config(postgresql: Connection) -> Generator[Connection, None, None]:
    """Applies envoy alembic migrations to a fresh test DB. Yields the psycopg Connection."""
    with environment_snapshot():
        os.environ["DATABASE_URL"] = generate_async_conn_str_from_connection(postgresql)
        upgrade()
        yield postgresql


@pytest.fixture
def pg_base_config(pg_empty_config: Connection) -> Generator[Connection, None, None]:
    """Extends pg_empty_config by seeding the minimum rows required for handler tests:
    the null aggregator (aggregator_id=0) that envoy uses as the owner of directly-connected devices."""
    now = datetime(2000, 1, 1, tzinfo=timezone.utc)
    with pg_empty_config.cursor() as cur:
        cur.execute(
            "INSERT INTO aggregator (aggregator_id, name, created_time, changed_time) VALUES (%s, %s, %s, %s)",
            (NULL_AGGREGATOR_ID, "NULL AGGREGATOR", now, now),
        )
    pg_empty_config.commit()
    yield pg_empty_config
