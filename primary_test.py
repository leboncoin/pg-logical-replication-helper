from unittest.mock import call

import pytest

from database import Database
from primary import Primary


def test_retrieve_db_infos(mocker):
    db = mocker.MagicMock()
    db.db_name = "db_name"
    primary = Primary(db)
    db.execute_query.side_effect = [
        [["schema_1"], ["schema_2"]], 
        [[321]], 
        [[123]]
    ]

    db_infos = primary.retrieve_db_infos(None)

    assert db_infos.db_schemas == ["schema_1", "schema_2"]
    assert db_infos.db_size == 321
    assert db_infos.db_tables == 123
    assert db_infos.schema_excluded_str == ""
    assert db.execute_query.call_args_list == [
        call("SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT ILIKE 'pg_%'"),
        call("SELECT pg_size_pretty(pg_database_size('db_name'))"),
        call("SELECT count(*) from pg_stat_user_tables")
    ]
    
    
def test_retrieve_db_infos_with_list_schema_excluded(mocker):
    db = mocker.MagicMock()
    db.db_name = "db_name"
    primary = Primary(db)
    db.execute_query.side_effect = [
        [["schema_3"]], 
        [[321]], 
        [[123]]
    ]
    db_infos = primary.retrieve_db_infos(["schema_1", "schema_2"])

    assert db_infos.db_schemas == ["schema_3"]
    assert db_infos.db_size == 321
    assert db_infos.db_tables == 123
    assert db_infos.schema_excluded_str == "'schema_1','schema_2'"
    assert db.execute_query.call_args_list == [
        call("SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT ILIKE 'pg_%' AND schema_name NOT IN ('schema_1','schema_2');"),
        call("SELECT pg_size_pretty(pg_database_size('db_name'))"),
        call("SELECT count(*) from pg_stat_user_tables")
    ]


def test_retrieve_db_infos_with_stub():
    db = DatabaseStub()
    primary = Primary(db)
    db.on_query_return("SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT ILIKE 'pg_%'",
                       [["schema_1"], ["schema_2"]])
    db.on_query_return("SELECT pg_size_pretty(pg_database_size('db_name'))",
                       [[321]])
    db.on_query_return("SELECT count(*) from pg_stat_user_tables",
                       [[123]])

    db_infos = primary.retrieve_db_infos(None)

    assert db_infos.db_schemas == ["schema_1", "schema_2"]
    assert db_infos.db_size == 321
    assert db_infos.db_tables == 123
    assert db_infos.schema_excluded_str == ""


def test_retrieve_db_infos_with_list_schema_excluded_with_stub():
    db = DatabaseStub()
    primary = Primary(db)
    db.on_query_return("SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT ILIKE 'pg_%' AND schema_name NOT IN ('schema_1','schema_2');",
                       [["schema_3"]])
    db.on_query_return("SELECT pg_size_pretty(pg_database_size('db_name'))",
                       [[321]])
    db.on_query_return("SELECT count(*) from pg_stat_user_tables",
                       [[123]])

    db_infos = primary.retrieve_db_infos(["schema_1", "schema_2"])

    assert db_infos.db_schemas == ["schema_3"]
    assert db_infos.db_size == 321
    assert db_infos.db_tables == 123
    assert db_infos.schema_excluded_str == "'schema_1','schema_2'"


def test_retrieve_db_infos_with_no_results_with_stub():
    db = DatabaseStub()
    primary = Primary(db)
    db.on_query_return("SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT ILIKE 'pg_%' AND schema_name NOT IN ('schema_1');",
                       [])
    db.on_query_return("SELECT pg_size_pretty(pg_database_size('db_name'))",
                       None)
    db.on_query_return("SELECT count(*) from pg_stat_user_tables",
                       [])

    db_infos = primary.retrieve_db_infos(["schema_1"])

    assert db_infos.db_schemas is None
    assert db_infos.db_size is None
    assert db_infos.db_tables is None
    assert db_infos.schema_excluded_str == "'schema_1'"


class DatabaseStub(Database):
    def __init__(self):
        self.recorded_queries = {}
        super().__init__("{string connection}", "db_name")

    def on_query_return(self, query, results):
        self.recorded_queries.update({query: results})
        
    def execute_query(self, query, fetch=True):
        try:
            if fetch:
                return self.recorded_queries[query]
        except KeyError:
            pytest.fail(f"The DatabaseStub don't have expected results for query \"{query}\".\nPlease use on_query_return method for this.")

    def get_db_connection(self):
        return None