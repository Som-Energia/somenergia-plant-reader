from tests.db_fixtures import engine, dbsession, dbconnection
from plant_reader import (
    read_dset,
    read_store_dset,
    get_config_dict,
    create_table,
    store_dset,
    flatten_historic_dset,
    read_dset_historic,
    store_dset_historic,
    read_store_dset_historic
)
import pytest
import datetime

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

def sample_historic_readings():
    readings = {
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
    }

    return readings

@pytest.fixture
def dset_config():
    config = get_config_dict('testing')

    return (config['base_url'], config['dset_api_key'], config['dset_api_groups_key'])

@pytest.fixture
def dset_tables(dbconnection):
    dset_table_name = 'dset_readings'
    create_table(dbconnection, dset_table_name)

@pytest.mark.skipif(False,reason="remote reads dset api, no rate limit but let's be nice")
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
    result = read_store_dset(dbconnection, base_url, apikey, schema="public")
    assert len(result) > 0


@pytest.mark.skipif(False,reason="remote reads dset api, no rate limit but let's be nice")
def test___store_dset__base_case(dbconnection, dset_config, dset_tables):
    readings = sample_readings()
    stored_readings = store_dset(dbconnection, readings, schema="public")
    print(stored_readings)
    assert len(stored_readings) > 0

@pytest.mark.skipif(False,reason="remote reads dset api, no rate limit but let's be nice")
def test___read_dset__base_case(dset_config):
    base_url, apikey, groupapikey = dset_config
    from_date = datetime.datetime(2023,8,1,13,0,tzinfo=datetime.timezone.utc)
    to_date = datetime.datetime(2023,8,1,13,5,tzinfo=datetime.timezone.utc)
    result = read_dset_historic(base_url, apikey, from_date, to_date)
    print(result)
    assert 'signals' in result
    assert len(result['signals']) > 0

@pytest.mark.skipif(False,reason="remote reads dset api, no rate limit but let's be nice")
def test___store_dset_historic__base_case(dbconnection, dset_config, dset_tables):
    readings = sample_historic_readings()
    params = {
        'from': datetime.datetime(2023,8,1,13,0,tzinfo=datetime.timezone.utc).isoformat(),
        'to' : datetime.datetime(2023,8,1,13,5,tzinfo=datetime.timezone.utc).isoformat()
    }
    stored_readings = store_dset_historic(dbconnection, readings, params=params, schema="public")
    print(stored_readings)
    assert len(stored_readings) > 0

@pytest.mark.skipif(False,reason="remote reads dset api, no rate limit but let's be nice")
def test___read_store_dset_historic__base_case(dbconnection, dset_config, dset_tables):
    base_url, apikey, groupapikey = dset_config
    from_date = datetime.datetime(2023,8,1,13,0,tzinfo=datetime.timezone.utc)
    to_date = datetime.datetime(2023,8,1,13,5,tzinfo=datetime.timezone.utc)
    result = read_store_dset_historic(dbconnection, base_url, apikey, from_date, to_date, schema="public")
    assert len(result) > 0

@pytest.mark.skipif(False,reason="remote reads dset api, no rate limit but let's be nice")
def test___flatten_historic_dset__base_case():
    readings = sample_historic_readings()
    flat_readings, flat_readings_meta = flatten_historic_dset(readings)

    assert len(flat_readings) == 3

    first_reading = flat_readings[0]
    assert 'data' not in first_reading
    assert first_reading['signal_id'] == 479502
    assert first_reading['signal_last_ts'] == '2023-08-01 13:00:00'
    assert first_reading['signal_last_value'] == 7308

