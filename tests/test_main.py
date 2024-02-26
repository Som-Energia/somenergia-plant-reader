import pytest

from plant_reader.utils import Environment
from scripts.main import get_readings


@pytest.mark.skip(reason="Not implemented")
def test___main__base_case():
    result = get_readings(Environment.TESTING)
    assert result is None
