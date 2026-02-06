from unittest.mock import call

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
    assert db.execute_query.call_args_list == [
        call("SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT ILIKE 'pg_%'"),
        call("SELECT pg_size_pretty(pg_database_size('db_name'))"),
        call("SELECT count(*) from pg_stat_user_tables")
    ]
    
