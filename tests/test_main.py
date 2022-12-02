from scripts.main import main_command

def test___main__base_case():
    dbapi = 'testing'
    result = main_command(dbapi)
    assert result is None
