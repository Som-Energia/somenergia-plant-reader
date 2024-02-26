from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from plant_reader.utils import get_config, Environment
import pytest


@pytest.fixture(scope="session")
def engine():
    """
    create a sqlalchemy engine
    """
    dbapi = get_config(Environment.TESTING).db_url

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


@pytest.fixture
def dbconnection(dbsession):
    return dbsession.connection()


@pytest.fixture
def dset_config():
    yield get_config(Environment.TESTING)
