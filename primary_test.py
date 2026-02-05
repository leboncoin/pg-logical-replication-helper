from primary import Primary


def test_retrieve_db_infos(mocker):
    db = mocker.MagicMock()
    primary = Primary(db)
    db.execute_query.return_value = [["schema_1"], ["schema_2"]]
    
    db_infos = primary.retrieve_db_infos([])
    
    assert db_infos.db_schemas == ["schema_1", "schema_2"]    
    