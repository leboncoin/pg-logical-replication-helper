from database_test import DatabaseStub
from secondary import Secondary


def test_get_subscription_name_returns_none_on_error():
    db = DatabaseStub()
    db.on_query_return("select subname from pg_subscription where subname like 'subscription_db_primary_%'", None)
    secondary = Secondary(db)

    subscription_name = secondary.get_subscription_name("db_primary")

    assert subscription_name is None


def test_get_subscription_name_returns_empty_str_when_no_record():
    db = DatabaseStub()
    db.on_query_return("select subname from pg_subscription where subname like 'subscription_db_primary_%'", [])
    secondary = Secondary(db)

    subscription_name = secondary.get_subscription_name("db_primary")

    assert subscription_name == ""


def test_get_subscription_name_returns_first_record():
    db = DatabaseStub()
    db.on_query_return("select subname from pg_subscription where subname like 'subscription_db_primary_%'", [["subscription_db_primary_1"], ["subscription_db_primary_2"]])
    secondary = Secondary(db)

    subscription_name = secondary.get_subscription_name("db_primary")

    assert subscription_name == "subscription_db_primary_1"
