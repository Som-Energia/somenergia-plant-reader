import httpx
import datetime
from sqlalchemy import table, column, MetaData, DateTime, Boolean, Column, String, Table
from sqlalchemy.dialects.postgresql import JSONB, insert
import json


class ApiException(Exception):
    pass


def create_table(conn, table_name, schema: str = "public"):
    meta = MetaData(conn)
    dbtable = Table(
        table_name,
        meta,
        Column("query_time", DateTime(timezone=True)),
        Column("endpoint", String),
        Column("params", String),
        Column("is_valid", Boolean),
        Column("group_id", String),
        Column("group_name", String),
        Column("group_code", String),
        Column("signal_id", String),
        Column("signal_code", String),
        Column("signal_description", String),
        Column("signal_frequency", String),
        Column("signal_type", String),
        Column("signal_is_virtual", String),
        Column("signal_last_ts", String),
        Column("signal_last_value", String),
        Column("signal_unit", String),
        schema=schema,
    )

    dbtable.create(conn, checkfirst=True)


def get_table(table_name, schema: str = "public"):
    return table(
        table_name,
        column("query_time", DateTime(timezone=True)),
        column("endpoint", String),
        column("params", String),
        column("is_valid", Boolean),
        column("group_id", String),
        column("group_name", String),
        column("group_code", String),
        column("signal_id", String),
        column("signal_code", String),
        column("signal_description", String),
        column("signal_frequency", String),
        column("signal_type", String),
        column("signal_is_virtual", String),
        column("signal_last_ts", String),
        column("signal_last_value", String),
        column("signal_unit", String),
        schema=schema,
    )


def read_dset(base_url, apikey):
    print(f"keys: {apikey}")

    url = f"{base_url}/api/groups"

    response = httpx.get(url, headers={"Authorization": apikey})

    response.raise_for_status()

    return response.json()[0]

def read_dset_historic(base_url, apikey, from_date : datetime.datetime, to_date : datetime.datetime):
    '''
    from_date and to_date are inclusive
    '''

    print(f"keys: {apikey}")

    url = f"{base_url}/api/data"
    params = {'from': from_date.isoformat(), 'to': to_date.isoformat()}
    response = httpx.get(url, params=params, headers={"Authorization": apikey})

    response.raise_for_status()

    return response.json()[0]

def insert_readings(conn, schema: str, request_meta, flat_readings_meta, flat_readings):

    dset_table_name = "dset_readings"

    dset_table = get_table(dset_table_name, schema=schema)

    # json_readings = json.dumps(readings, indent=4, sort_keys=True, default=str)

    reading_data = [
        {**request_meta, **flat_readings_meta, **reading} for reading in flat_readings
    ]

    insert_statement = (
        insert(dset_table)
        .values(reading_data)
        .returning(
            dset_table.c.group_id,
            dset_table.c.signal_id,
            dset_table.c.signal_code,
            dset_table.c.signal_last_ts,
            dset_table.c.signal_last_value,
        )
    )
    result = conn.execute(insert_statement)

    return [dict(r) for r in result.all()]

def flatten_historic_dset(readings):
    '''
    takes the last_reading, copies it and creates a fake last_reading from the historic data
    at the historic point in time, as if it was live read then
    '''
    flat_readings_with_unflattened_data = readings.pop("signals", [])
    flat_readings_meta = readings
    flat_readings = flat_readings_with_unflattened_data

    for reading in flat_readings:
        historic_readings = reading.pop('data',[])
        if not historic_readings:
            # no historic data in the requested from-to range
            # TODO is this what we want? we don't have the actual from-to nor the actual requested ts
            # TODO should we print a warning? Raise?
            reading['signal_last_ts'] = None
            reading['signal_last_value'] = None
        else:
            historic_reading = historic_readings[0]
            reading['signal_last_ts'] = historic_reading['ts']
            reading['signal_last_value'] = historic_reading['value']

    return flat_readings, flat_readings_meta

def store_dset_historic(conn, readings, params: str, schema: str):
    if "signals" not in readings:
        raise ApiException(f"Readings: {readings}")

    # flat version
    flat_readings, flat_readings_meta = flatten_historic_dset(readings)

    request_meta = {
        "query_time": datetime.datetime.now(datetime.timezone.utc),
        "endpoint": "/api/data",
        "params": params,
        "is_valid": True,
    }

    return insert_readings(conn, schema, request_meta, flat_readings_meta, flat_readings)

def store_dset(conn, readings, schema: str):
    if "signals" not in readings:
        raise ApiException(f"Readings: {readings}")

    dset_table_name = "dset_readings"

    dset_table = get_table(dset_table_name, schema=schema)

    # flat version
    flat_readings = readings.pop("signals", [])
    flat_readings_meta = readings

    request_meta = {
        "query_time": datetime.datetime.now(datetime.timezone.utc),
        "endpoint": "/api/groups",
        "params": "",
        "is_valid": True,
    }

    # json_readings = json.dumps(readings, indent=4, sort_keys=True, default=str)

    reading_data = [
        {**request_meta, **flat_readings_meta, **reading} for reading in flat_readings
    ]

    insert_statement = (
        insert(dset_table)
        .values(reading_data)
        .returning(
            dset_table.c.group_id,
            dset_table.c.signal_id,
            dset_table.c.signal_code,
            dset_table.c.signal_last_ts,
            dset_table.c.signal_last_value,
        )
    )
    result = conn.execute(insert_statement)

    return [dict(r) for r in result.all()]


def read_store_dset(conn, base_url, apikey, schema):
    readings = read_dset(base_url, apikey)

    return store_dset(conn, readings, schema)

def read_store_dset_historic(conn, base_url, apikey, from_date, to_date, schema):

    readings = read_dset_historic(base_url, apikey, from_date, to_date)

    params = {'from': from_date.isoformat(), 'to': to_date.isoformat()}

    return store_dset_historic(conn, readings, params, schema)
