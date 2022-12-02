from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from plant_reader.utils import get_config
import pytest

@pytest.fixture(scope="session")
def engine():
    '''
    create a sqlalchemy engine
    '''
    dbapi = get_config('testing')

    return create_engine(dbapi)

@pytest.fixture
def dbsession(engine):
    """Returns an sqlalchemy session, and after the test tears down everything properly."""
    connection = engine.connect()
    # begin the nested transaction
    transaction = connection.begin()
    # use the connection with the already started transaction
    session = Session(bind=connection)

    yield session

    session.close()
    # roll back the broader transaction
    transaction.rollback()
    # put back the connection to the connection pool
    connection.close()
