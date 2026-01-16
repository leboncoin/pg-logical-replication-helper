import os

import psycopg
import pytest
from testcontainers.compose import DockerCompose
from testcontainers.core.wait_strategies import LogMessageWaitStrategy

from replication_start import main


@pytest.fixture(scope="module", autouse=True)
def setup(request):
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

    os.environ["CONN_DB_PRIMARY_FULL"]="postgresql://foo:foopwd@pgfoo:5432/foo_db"

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
    main("test", get_source_url(), "foo_db", get_destination_url(), "bar_db", False, 'information_schema')
    
    with psycopg.connect(get_destination_url()) as conn:
        with conn.cursor() as cur:
            cur.execute("select * from table_to_replicate")
            cur.execute("select * from table_to_replicate2")
            cur.execute("select * from table_to_replicate3")
            conn.commit()
                      
