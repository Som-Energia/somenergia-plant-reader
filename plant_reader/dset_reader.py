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
