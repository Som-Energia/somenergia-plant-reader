from tests.db_fixtures import dbsession, engine
from plant_reader import main_read_store

def test___main__base_case(dbsession):
    result = main_read_store(dbsession)
    assert result == 0
