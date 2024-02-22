import pytest
from plant_reader import main_read_store


@pytest.mark.skip(reason="remote reads plants in production")
def test___main__base_case(dbsession):
    result = main_read_store(dbsession)
    assert result == 0
