
import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--dset", action="store_true", default=False, help="run dset api tests"
    )


def pytest_configure(config):
    config.addinivalue_line("markers", "dset: mark test as against dset api to run")


def pytest_collection_modifyitems(config, items):
    if config.getoption("--dset"):
        # --dset given in cli: do not skip slow tests
        return
    skip_dset = pytest.mark.skip(reason="need --dset option to run")
    for item in items:
        if "dset" in item.keywords:
            item.add_marker(skip_dset)