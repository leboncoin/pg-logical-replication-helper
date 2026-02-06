import datetime
import os
import re
import subprocess
from typing import Any

import psycopg
from psycopg import sql
import pytest
from testcontainers.compose import DockerCompose
from testcontainers.core.wait_strategies import LogMessageWaitStrategy

import replication_start

FAKE_TIME = datetime.datetime(2026, 1, 22, 10, 1, 25)


@pytest.fixture(scope="module", autouse=True)
def setup(request):
    pg_dump_version = detect_pg_dump_version()
    if "POSTGRES_PRIMARY_VERSION" not in os.environ:
        os.environ["POSTGRES_PRIMARY_VERSION"] = pg_dump_version

    if "POSTGRES_SECONDARY_VERSION" not in os.environ:
        os.environ["POSTGRES_SECONDARY_VERSION"] = pg_dump_version

    compose = DockerCompose(
        context=".",
        compose_file_name="docker-compose.yml",
        pull=True,
        wait=True
    ).waiting_for({
        "pgfoo": LogMessageWaitStrategy("database system is ready to accept connections"),
        "pgbar": LogMessageWaitStrategy("database system is ready to accept connections")
    })
    compose.start()

    def remove_container():
        compose.stop()

    os.environ["CONN_DB_PRIMARY_FULL"] = "postgresql://foo:foopwd@pgfoo:5432/foo_db"

    request.addfinalizer(remove_container)


@pytest.fixture(scope="function", autouse=True)
def patch_datetime_now(monkeypatch):
    class MockedDateTime(datetime.datetime):
        @classmethod
        def now(cls, **kwargs):
            return FAKE_TIME

    monkeypatch.setattr(datetime, 'datetime', MockedDateTime)
    monkeypatch.setattr(replication_start, "WAITING_PROGRESS_IN_SECONDS", 0)


@pytest.fixture(scope="function", autouse=True)
def setup_data(request):
    date_start = FAKE_TIME.strftime("%Y%m%d_%H%M%S")
    unique_name = f"foo_db_{date_start}"
    def clear_data():
        with psycopg.connect(get_secondary_url(), autocommit=True) as conn:
            conn.execute(f"ALTER SUBSCRIPTION subscription_{unique_name} DISABLE")
            conn.execute(f"ALTER SUBSCRIPTION subscription_{unique_name} SET (slot_name=none)")
            conn.execute(f"DROP SUBSCRIPTION subscription_{unique_name}")
            conn.execute(f"select pid, usename, state from pg_stat_activity where datname = 'foo_db'")

        with psycopg.connect(get_secondary_url("postgres"), autocommit=True) as conn:
            conn.execute("DROP DATABASE bar_db")
            conn.execute("CREATE DATABASE bar_db")

        with psycopg.connect(get_source_url(), autocommit=True) as conn:
            conn.execute(f"SELECT pg_drop_replication_slot('subscription_{unique_name}')")
            conn.execute(f"DROP PUBLICATION publication_{unique_name}")

    request.addfinalizer(clear_data)


def get_source_url() -> str:
    host = "localhost"
    port = "15431"
    username = "foo"
    password = "foopwd"
    database = "foo_db"
    return f"host={host} dbname={database} user={username} password={password} port={port}"


def get_secondary_url(db="bar_db") -> str:
    host = "localhost"
    port = "15432"
    username = "bar"
    password = "barpwd"
    database = db
    return f"host={host} dbname={database} user={username} password={password} port={port}"


@pytest.mark.slow
def test_main():
    replication_start.main("test", get_source_url(), "foo_db", get_secondary_url(), "bar_db", None)

    with psycopg.connect(get_secondary_url(), autocommit=True) as conn:
        assert count_records(conn, "included", "table_to_replicate") == 2
        assert count_records(conn, "included", "table_to_replicate2") == 2
        assert count_records(conn, "excluded", "table_to_replicate3") == 2


@pytest.mark.slow
def test_main_with_excluded_schema():
    replication_start.main("test", get_source_url(), "foo_db", get_secondary_url(), "bar_db", ["excluded"])

    with psycopg.connect(get_secondary_url(), autocommit=True) as conn:
        with conn.cursor() as cur:
            results = cur.execute("select schemaname, relname from pg_stat_user_tables where relname <> 'spatial_ref_sys'")
            for schema, table in results:
                assert schema != "excluded"
                assert table in ["table_to_replicate", "table_to_replicate2"]
                assert count_records(conn, schema, table) == 2


def detect_pg_dump_version() -> str | Any:
    pg_dump_version_output = subprocess.check_output(["pg_dump", "--version"], text=True, stderr=None)
    print("Detected pg_dump version : " + pg_dump_version_output)
    pg_dump_version = re.match("pg_dump \\(PostgreSQL\\) (\\d{2})\\..*", pg_dump_version_output).group(1)
    print("Extracted pg_dump version : " + pg_dump_version)
    return pg_dump_version


def count_records(conn: psycopg.Connection[Any], schema: str, table: str) -> int:
    query = sql.Composed([sql.SQL("SELECT count(*) FROM "), sql.Identifier(schema, table)])
    result = conn.execute(query).fetchone()
    return result[0]
