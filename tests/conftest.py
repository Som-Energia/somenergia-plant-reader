import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from plant_reader.utils import Environment, get_config


def pytest_addoption(parser):
    parser.addoption(
        "--dset",
        action="store_true",
        default=False,
        help="run dset api tests",
    )


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "dset: mark test as against dset api to run",
    )


def pytest_collection_modifyitems(config, items):
    if config.getoption("--dset"):
        # --dset given in cli: do not skip slow tests
        return
    skip_dset = pytest.mark.skip(reason="need --dset option to run")
    for item in items:
        if "dset" in item.keywords:
            item.add_marker(skip_dset)


@pytest.fixture(scope="session")
def engine():
    """
    create a sqlalchemy engine
    """
    dbapi = get_config(Environment.TESTING).db_url

    return create_engine(dbapi)


@pytest.fixture
def dbsession(engine):
    """Returns an sqlalchemy session, and tears down everything afterwards"""
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


@pytest.fixture
def cli_runner():
    from typer.testing import CliRunner

    return CliRunner()
