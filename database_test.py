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
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

    query = "SELECT 1;"
    db.execute_query(query, False)

    mock_cursor.execute.assert_called_once_with(query)
    mock_cursor.fetchall.assert_not_called()
    mock_connection.close.assert_called_once()


def test_database_execute_query_with_fetch(mocker):
    db = Database("{string connection}", "db_name")
    mock_connection = mocker.MagicMock()
    mocker.patch("psycopg.connect", return_value=mock_connection)
    mock_cursor = mocker.MagicMock()
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
    mock_cursor.fetchall.return_value = [1]

    query = "SELECT 1;"
    result = db.execute_query(query)

    assert result == [1]
    mock_cursor.execute.assert_called_once_with(query)
    mock_cursor.fetchall.assert_called_once()
    mock_connection.close.assert_called_once()


def test_database_execute_query_on_error(mocker, capsys):
    db = Database("{string connection}", "db_name")
    mock_connection = mocker.MagicMock()
    mocker.patch("psycopg.connect", return_value=mock_connection)
    mock_connection.cursor.return_value.__enter__.side_effect = psycopg.Error("<expected error>")

    query = "SELECT 1;"
    result = db.execute_query(query, False)

    assert result is None
    assert capsys.readouterr().err == "Error <expected error> with query 'SELECT 1;' on host '{string connection}'\n"
    mock_connection.close.assert_called_once()
