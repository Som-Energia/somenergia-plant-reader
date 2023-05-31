from scripts.main import get_readings

def _test___main__base_case():
    dbapi = 'testing'
    result = get_readings(dbapi)
    assert result is None
