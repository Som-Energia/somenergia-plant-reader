from tests.db_fixtures import engine, dbsession, dbconnection
from plant_reader import (
    read_dset,
    read_store_dset,
    get_config_dict,
    create_table,
    create_response_table,
    localize_time_range,
    store_dset,
    store_dset_response,
    get_dset_to_db
)
import pytest
import datetime
import httpx
import pytz

def sample_readings():
    readings = {
        'group_id': 867,
        'group_code': 'I7256',
        'group_name': 'SOMENERGIA',
        'signals': [
        {
            'signal_id': 479516,
            'signal_code': 's7382',
            'signal_description': 'ETOTAL',
            'signal_frequency': '15 minutes',
            'signal_type': 'accumulated',
            'signal_is_virtual': False,
            'signal_last_ts': '2023-05-31 11:00:00',
            'signal_last_value': 11286848,
            'signal_unit': 'kWh',
        },
        {
            'signal_id': 479512,
            'signal_code': 's7380',
            'signal_description': 'SCB1',
            'signal_frequency': '15 minutes',
            'signal_type': 'absolute',
            'signal_is_virtual': False,
            'signal_last_ts': '2023-05-31 11:00:00',
            'signal_last_value': 130.7,
            'signal_unit': 'A',
        },
        {
            'signal_id': 479510,
            'signal_code': 's7379',
            'signal_description': 'VTE',
            'signal_frequency': '15 minutes',
            'signal_type': 'absolute',
            'signal_is_virtual': False,
            'signal_last_ts': '2023-05-31 11:00:00',
            'signal_last_value': 628.0,
            'signal_unit': 'V',
        },
        {
            'signal_id': 479504,
            'signal_code': 's7376',
            'signal_description': 'Eayer',
            'signal_frequency': '15 minutes',
            'signal_type': 'accumulated',
            'signal_is_virtual': False,
            'signal_last_ts': '2023-05-31 11:00:00',
            'signal_last_value': 19806.0,
            'signal_unit': 'kWh',
        },
        {
            'signal_id': 479506,
            'signal_code': 's7377',
            'signal_description': 'VST',
            'signal_frequency': '15 minutes',
            'signal_type': 'absolute',
            'signal_is_virtual': False,
            'signal_last_ts': '2023-05-31 11:00:00',
            'signal_last_value': 625.0,
            'signal_unit': 'V',
        },
        {
            'signal_id': 479508,
            'signal_code': 's7378',
            'signal_description': 'VRS',
            'signal_frequency': '15 minutes',
            'signal_type': 'absolute',
            'signal_is_virtual': False,
            'signal_last_ts': '2023-05-31 11:00:00',
            'signal_last_value': 628.0,
            'signal_unit': 'V',
        },
        {
            'signal_id': 479514,
            'signal_code': 's7381',
            'signal_description': 'SCB2',
            'signal_frequency': '15 minutes',
            'signal_type': 'absolute',
            'signal_is_virtual': False,
            'signal_last_ts': '2023-05-31 11:00:00',
            'signal_last_value': 107.5,
            'signal_unit': 'A',
        },
        {
            'signal_id': 479502,
            'signal_code': 's7375',
            'signal_description': 'Ediaria',
            'signal_frequency': '15 minutes',
            'signal_type': 'accumulated',
            'signal_is_virtual': False,
            'signal_last_ts': '2023-05-31 11:00:00',
            'signal_last_value': 3785.0,
            'signal_unit': 'kWh',
        },
        ],
    }
    return readings

def sample_data_response():
    response_body = [{
        'group_id': 867,
        'group_code': 'I7256',
        'group_name': 'SOMENERGIA',
        'signals': [{'signal_id': 479502,
          'signal_code': 's7375',
          'signal_description': 'Ediaria',
          'signal_frequency': '15 minutes',
          'signal_type': 'accumulated',
          'signal_is_virtual': False,
          'signal_last_ts': '2023-08-16 13:00:00',
          'signal_last_value': 21.0,
          'signal_unit': 'kWh',
          'data': [{'ts': '2023-08-01 13:15:00', 'value': 7308},
           {'ts': '2023-08-01 13:00:00', 'value': 7308}]},
         {'signal_id': 479504,
          'signal_code': 's7376',
          'signal_description': 'Eayer',
          'signal_frequency': '15 minutes',
          'signal_type': 'accumulated',
          'signal_is_virtual': False,
          'signal_last_ts': '2023-08-16 13:00:00',
          'signal_last_value': 19614.0,
          'signal_unit': 'kWh',
          'data': [{'ts': '2023-08-01 13:15:00', 'value': 23928},
           {'ts': '2023-08-01 13:00:00', 'value': 23928}]},
         {'signal_id': 479510,
          'signal_code': 's7379',
          'signal_description': 'VTE',
          'signal_frequency': '15 minutes',
          'signal_type': 'absolute',
          'signal_is_virtual': False,
          'signal_last_ts': '2023-07-14 14:15:00',
          'signal_last_value': 621.0,
          'signal_unit': 'V',
          'data': []}]
    }]

    return response_body

@pytest.fixture
def dset_config():
    config = get_config_dict('testing')

    return (config['base_url'], config['dset_api_key'], config['dset_api_groups_key'])

@pytest.fixture
def dset_tables(dbconnection):
    dset_table_name = 'dset_readings'
    create_table(dbconnection, dset_table_name)
    dset_table_name = 'dset_responses'
    create_response_table(dbconnection, dset_table_name)

@pytest.mark.dset
def test___read_dset__base_case(dset_config):
    base_url, apikey, groupapikey = dset_config
    results = read_dset(base_url, apikey)
    result = results[0]
    print(result)
    assert 'signals' in result
    assert len(result['signals']) > 0
    # check expected keys

@pytest.mark.dset
@pytest.mark.skipif(True,reason="deprecated by read_store_historic_dset")
def test___read_store_dset__base_case(dbconnection, dset_config, dset_tables):
    base_url, apikey, groupapikey = dset_config
    results = read_store_dset(dbconnection, base_url, apikey, schema="public")
    assert len(results[0]) > 0


@pytest.mark.dset
@pytest.mark.skipif(True,reason="deprecated by store_historic_dset")
def test___store_dset__base_case(dbconnection, dset_config, dset_tables):
    readings = sample_readings()
    stored_readings = store_dset(dbconnection, readings, schema="public")
    print(stored_readings)
    assert len(stored_readings) > 0

    # dset data specific
# @pytest.mark.dset
def test___dset_schema_changes(dset_config):
    base_url, apikey, groupapikey = dset_config
    endpoint = f'{base_url}/api/data'
    from_ts = datetime.datetime(2023,10,1,13,0,tzinfo=datetime.timezone.utc)
    to_ts = datetime.datetime(2023,10,1,13,5,tzinfo=datetime.timezone.utc)

    to_ts_exclusive = to_ts - datetime.timedelta(seconds=1)

    from_ts_local, to_ts_local = localize_time_range(from_ts, to_ts_exclusive)

    params = {
        'from': from_ts_local.isoformat(),
        'to' : to_ts_local.isoformat(),
        'sig_detail': True
    }

    response = httpx.get(endpoint, params=params, headers={"Authorization": apikey},  timeout=10.0)

    response.raise_for_status()

    res = response.json()

    # fragile

    # groups (plants + 2 legacy)
    assert len(res) > 0

    # currently the fist group is SE_llanillos
    # pretty fragile
    assert res[0]['group_id'] == 930
    assert len(res[0]['signals']) > 0
    assert 'data' in res[0]['signals'][0]
    #assert res[0]['signals'][0]['data'] # fragile
    assert 'data' in res[0]['signals'][0]

def test___store_dset_historic__base_case(dbconnection, dset_config, dset_tables):
    # sample has [2023-08-01 13:00,2023-08-01 13:15]
    # we will usually ask for to-1second to make the interval right-opened
    response_body = sample_data_response()
    params = {}
    endpoint = "https://example.com"
    response = httpx.Response(200, json=response_body, headers={}, request=httpx.Request("GET", url=endpoint, params=params))
    stored_readings = store_dset_response(dbconnection, response, endpoint, params=params, schema="public")
    print(stored_readings)
    assert len(stored_readings) > 0


def test___localize_time_range():
    from_ts = datetime.datetime(2023,8,1,13,0,tzinfo=datetime.timezone.utc)
    to_ts = datetime.datetime(2023,8,1,13,5,tzinfo=datetime.timezone.utc)

    to_ts_exclusive = to_ts - datetime.timedelta(seconds=1)

    from_ts_local, _ = localize_time_range(from_ts, to_ts_exclusive)
    assert from_ts_local.tzinfo.zone == pytz.timezone('Europe/Madrid').zone
    assert from_ts_local.astimezone(datetime.timezone.utc) == from_ts.astimezone(datetime.timezone.utc)

@pytest.mark.dset
def test___read_store_dset_historic__base_case(dbconnection, dset_config, dset_tables):
    base_url, apikey, groupapikey = dset_config
    endpoint = f'{base_url}/api/data'
    from_ts = datetime.datetime(2023,10,20,13,0,tzinfo=datetime.timezone.utc)
    to_ts = datetime.datetime(2023,10,20,13,5,tzinfo=datetime.timezone.utc)

    to_ts_exclusive = to_ts - datetime.timedelta(seconds=1)

    from_ts_local, to_ts_local = localize_time_range(from_ts, to_ts_exclusive)
    params = {
        'from': from_ts_local.isoformat(),
        'to' : to_ts_local.isoformat(),
        'sig_detail' : True
    }

    # dset at the moment DOES NOT support aware timstamps, it just discards them,
    # it's responsibility of the caller to switch with localize_time_range

    result = get_dset_to_db(dbconnection, endpoint, apikey, params, schema="public")
    assert len(result) > 0
