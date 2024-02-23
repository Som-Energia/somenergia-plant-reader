import pytest
from plant_reader import main_read_store


@pytest.mark.skipif(True, reason="remote reads plants in production, don't abuse")
def test___main__base_case(dbsession):
    result = main_read_store(dbsession)
    assert result == 0
