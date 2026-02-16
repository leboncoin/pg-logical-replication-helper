from unittest.mock import sentinel

import psycopg
import pytest

from database import Database


def test_database_connection_return_the_connection_created_with_psycopg(mocker):
    db = Database("{string connection}", "db_name")
    mocker.patch("psycopg.connect", return_value=sentinel.some_connection)

    connection = db.get_db_connection()

    assert connection is sentinel.some_connection


def test_database_connection_exit_and_log_exception(mocker, capsys):
    db = Database("{string connection}", "db_name")
    mocker.patch("psycopg.connect", side_effect=psycopg.Error("<expected error>"))
    mocked_sys_exit = mocker.patch("sys.exit")

    db.get_db_connection()

    mocked_sys_exit.assert_called_once_with(1)
    assert capsys.readouterr().err == "Error <expected error> on connection string {string connection}\n"


def test_database_execute_query_return_none_on_connection_none(mocker):
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
    mock_cursor.fetchall.return_value = sentinel.some_array

    query = "SELECT 1;"
    result = db.execute_query(query)

    assert result == sentinel.some_array
    mock_cursor.execute.assert_called_once_with(query)
    mock_cursor.fetchall.assert_called_once()
    mock_connection.close.assert_called_once()


def test_database_execute_query_on_error(mocker, capsys):
    db = Database("{string connection}", "db_name")
    mock_connection = mocker.MagicMock()
    mocker.patch("psycopg.connect", return_value=mock_connection)
    mock_connection.cursor.return_value.__enter__.side_effect = psycopg.Error("<expected error>")

    query = "SELECT 1;"
    result = db.execute_query(query)

    assert result is None
    assert capsys.readouterr().err == "Error <expected error> with query 'SELECT 1;' on host '{string connection}'\n"
    mock_connection.close.assert_called_once()


def test_database_execute_query_rollback_on_error_commit_on_success(mocker):
    db = Database("{string connection}", "db_name")
    mock_connection = mocker.MagicMock()
    mock_cursor = mocker.MagicMock()
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

    mock_db_connect = mocker.MagicMock()
    mock_db_connect.return_value.__enter__.return_value = mock_connection
    with mocker.patch("psycopg.connect", mock_db_connect): 
        query = "SELECT 1;"
        db.execute_query_rollback_on_error(query)

    mock_cursor.execute.assert_called_once_with(query)
    mock_connection.commit.assert_called_once()
    mock_connection.rollback.assert_not_called()


def test_database_execute_query_rollback_on_error(mocker, capsys):
    db = Database("{string connection}", "db_name")
    mock_connection = mocker.MagicMock()
    mock_cursor = mocker.MagicMock()
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
    mock_cursor.execute.side_effect = psycopg.Error("<expected error>")

    mock_db_connect = mocker.MagicMock()
    mock_db_connect.return_value.__enter__.return_value = mock_connection
    with mocker.patch("psycopg.connect", mock_db_connect):
        query = "SELECT 1;"
        db.execute_query_rollback_on_error(query)

    mock_cursor.execute.assert_called_once_with(query)
    mock_connection.commit.assert_not_called()
    mock_connection.rollback.assert_called_once()
    assert capsys.readouterr().err == "An error occurred: <expected error>\n"


class DatabaseStub(Database):
    def __init__(self):
        self.recorded_queries = {}
        super().__init__("{string connection}", "db_name")

    def on_query_return(self, query, results):
        self.recorded_queries.update({query: results})
        
    def execute_query(self, query, fetch=True):
        try:
            return self.recorded_queries[query]
        except KeyError:
            pytest.fail(f"The DatabaseStub don't have expected results for query \"{query}\".\nPlease use on_query_return method for this.")

    def get_db_connection(self):
        return None
