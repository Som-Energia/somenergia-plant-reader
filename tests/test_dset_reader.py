from tests.db_fixtures import engine, dbsession, dbconnection
from plant_reader import read_dset, read_store_dset, get_config_dict, create_table
import pytest

@pytest.fixture
def dset_config():
    config = get_config_dict('testing')

    return (config['base_url'], config['dset_api_key'], config['dset_api_groups_key'])

@pytest.fixture
def dset_tables(dbconnection):
    dset_table_name = 'dset_readings'
    create_table(dbconnection, dset_table_name)

@pytest.mark.skipif(True,reason="remote reads dset api, no rate limit but let's be nice")
def test___read_dset__base_case(dset_config):
    base_url, apikey, groupapikey = dset_config
    result = read_dset(base_url, apikey)
    print(result)
    assert 'signals' in result
    assert len(result['signals']) > 0
    # check expected keys

@pytest.mark.skipif(False,reason="remote reads dset api, no rate limit but let's be nice")
def test___read_store_dset__base_case(dbconnection, dset_config, dset_tables):
    base_url, apikey, groupapikey = dset_config
    result = read_store_dset(dbconnection, base_url, apikey)
    assert len(result) > 0
