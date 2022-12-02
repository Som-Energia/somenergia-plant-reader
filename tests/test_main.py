from scripts import main

def test___main__base_case():
    dbapi = 'testing'
    result = main(dbapi)
    assert result is None
