import psycopg

from database import Database


def test_database_connection_return_none(mocker):
    db = Database("{string connection}", "db_name")
    mocker.patch("psycopg.connect", return_value=None)

    connection = db.get_db_connection()

    assert connection is None


def test_database_connection_return_exception(mocker, capsys):
    db = Database("{string connection}", "db_name")
    mocker.patch("psycopg.connect",
                 side_effect=psycopg.Error("<expected error>"))
    mocked_sys_exit = mocker.patch("sys.exit")

    db.get_db_connection()

    mocked_sys_exit.assert_called_once_with(1)
    assert capsys.readouterr(
    ).err == "Error <expected error> on connection string {string connection}\n"


def test_execute_query_return_none_on_connection_none(mocker):
    db = Database("{string connection}", "db_name")
    mocker.patch("psycopg.connect", return_value=None)

    result = db.execute_query("SELECT 1;")

    assert result is None


def test_database_execute_query_without_fetch(mocker):
    db = Database("{string connection}", "db_name")
    mock_connection = mocker.MagicMock()
    mocker.patch("psycopg.connect", return_value=mock_connection)

    mock_cursor = mocker.MagicMock()
    mock_connection.cursor.return_value = mock_cursor
    # mocker.patch("mock_connection.cursor", return_value=mock_cursor)

    query = "SELECT 1;"
    result = db.execute_query(query, False)

    mock_cursor.execute.assert_called_once_with(query)
    mock_cursor.fetchall.assert_called_count == 0

    # try:
    #     conn.autocommit = True
    #     with conn.cursor() as cur:
    #         cur.execute(query)
    #         if fetch:
    #             results = cur.fetchall()
    #             return results
