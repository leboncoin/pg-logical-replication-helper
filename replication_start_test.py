import os
import re
import subprocess
from typing import Any

import psycopg
from psycopg import sql
import pytest
from testcontainers.compose import DockerCompose
from testcontainers.core.wait_strategies import LogMessageWaitStrategy

from replication_start import main


@pytest.fixture(scope="function", autouse=True)
def setup(request):
    os.environ["POSTGRES_VERSION"] = detect_pg_dump_version()

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


def get_source_url() -> str:
    host = "localhost"
    port = "15431"
    username = "foo"
    password = "foopwd"
    database = "foo_db"
    return f"host={host} dbname={database} user={username} password={password} port={port}"


def get_destination_url() -> str:
    host = "localhost"
    port = "15432"
    username = "bar"
    password = "barpwd"
    database = "bar_db"
    return f"host={host} dbname={database} user={username} password={password} port={port}"


def test_main():
    main("test", get_source_url(), "foo_db", get_destination_url(), "bar_db", None)

    with psycopg.connect(get_destination_url(), autocommit=True) as conn:
        assert count_records(conn, "included", "table_to_replicate") == 2
        assert count_records(conn, "included", "table_to_replicate2") == 2
        assert count_records(conn, "excluded", "table_to_replicate3") == 2


def test_main_with_excluded_schema():
    main("test", get_source_url(), "foo_db", get_destination_url(), "bar_db", ["excluded"])

    with psycopg.connect(get_destination_url(), autocommit=True) as conn:
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


def count_records(conn: psycopg.Connection[Any], schema:str, table: str) -> int:
    query = sql.SQL("SELECT count(*) FROM {}").format(sql.Identifier(schema, table))
    result = conn.execute(query).fetchone()
    return result[0]
