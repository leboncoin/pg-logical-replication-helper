import os

import psycopg
import pytest
from testcontainers.postgres import PostgresContainer
from replication_start import main

postgres_container = PostgresContainer("postgres:16-alpine")


@pytest.fixture(scope="module", autouse=True)
def setup(request):
    postgres_container.start()

    def remove_container():
        postgres_container.stop()

    request.addfinalizer(remove_container)

    os.environ["DB_CONN"] = postgres_container.get_connection_url()
    os.environ["DB_HOST"] = postgres_container.get_container_host_ip()
    os.environ["DB_PORT"] = str(postgres_container.get_exposed_port(5432))
    os.environ["DB_USERNAME"] = postgres_container.username
    os.environ["DB_PASSWORD"] = postgres_container.password
    os.environ["DB_NAME"] = postgres_container.dbname

    with psycopg.connect(get_connection_url()) as conn: 
        with conn.cursor() as cur:
            cur.execute("""
            create table db_test (
                id serial PRIMARY KEY,
                label varchar not null)
            """)

def get_connection_url() -> str:
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    username = os.getenv("DB_USERNAME", "postgres")
    password = os.getenv("DB_PASSWORD", "postgres")
    database = os.getenv("DB_NAME", "postgres")
    return f"host={host} dbname={database} user={username} password={password} port={port}"


def test_main():
    with psycopg.connect(get_connection_url()) as conn:
        with conn.cursor() as cur:
            cur.execute("insert into db_test (label) values ('test')")
            conn.commit()
            
