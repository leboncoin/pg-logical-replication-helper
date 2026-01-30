import psycopg

from database import Database


def test_database_connection_return_none(mocker):
    db = Database("{string connection}", "db_name")
    mocker.patch("psycopg.connect", return_value=None)

    connection = db.get_db_connection()
    
    assert connection is None
    

def test_database_connection_return_exception(mocker, capsys):
    db = Database("{string connection}", "db_name")
    mocker.patch("psycopg.connect", side_effect=psycopg.Error("<expected error>"))
    mocked_sys_exit = mocker.patch("sys.exit")

    db.get_db_connection()
    
    mocked_sys_exit.assert_called_once_with(1)
    assert capsys.readouterr().err == "Error <expected error> on connection string {string connection}\n"
