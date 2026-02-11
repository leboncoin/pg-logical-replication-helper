from database_test import DatabaseStub
from primary import Primary


def test_retrieve_db_infos():
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


def test_retrieve_db_infos_with_list_schema_excluded():
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


def test_retrieve_db_infos_with_no_results():
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


