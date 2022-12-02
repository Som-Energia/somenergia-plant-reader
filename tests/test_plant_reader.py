from tests.db_fixtures import dbsession, engine
from plant_reader import main_function

def test___main__base_case(dbsession):
    result = main_function(dbsession)
    assert result == 0
