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

    request.addfinalizer(remove_container)


def get_connection_url() -> str:
    host = "localhost"
    port = "15431"
    username = "foo"
    password = "foopwd"
    database = "foo_db"
    return f"host={host} dbname={database} user={username} password={password} port={port}"


def test_main():
    with psycopg.connect(get_connection_url()) as conn:
        with conn.cursor() as cur:
            cur.execute("select * from table_to_replicate")
            conn.commit()
            
