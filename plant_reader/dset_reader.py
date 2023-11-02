import httpx
import datetime
from sqlalchemy import table, column, MetaData, DateTime, Boolean, Column, String, Table
from sqlalchemy.dialects.postgresql import JSONB, insert
import pytz


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
        Column("signal_tz", String, nullable=True),
        Column("signal_last_ts", String),
        Column("signal_last_value", String),
        Column("signal_unit", String),
        schema=schema,
    )

    dbtable.create(conn, checkfirst=True)


def create_response_table(conn, table_name, schema: str = "public"):
    meta = MetaData(conn)
    dbtable = Table(
        table_name,
        meta,
        Column("query_time", DateTime(timezone=True)),
        Column("endpoint", String),
        Column("params", JSONB),
        Column("is_valid", Boolean),
        Column("response", JSONB),
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
        column("signal_tz", String),
        column("signal_last_ts", String),
        column("signal_last_value", String),
        column("signal_unit", String),
        schema=schema,
    )


def get_response_table(table_name, schema: str = "public"):
    return table(
        table_name,
        column("query_time", DateTime(timezone=True)),
        column("endpoint", String),
        column("params", JSONB),
        column("is_valid", Boolean),
        column("response", JSONB),
        schema=schema,
    )


def read_dset(base_url, apikey):
    print(f"keys: {apikey}")

    url = f"{base_url}/api/groups"

    response = httpx.get(url, headers={"Authorization": apikey})

    response.raise_for_status()

    return response.json()


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


def store_dset_response(
    conn,
    response: httpx.Response,
    endpoint: str,
    params: str,
    schema: str,
):
    request_data = {
        "query_time": datetime.datetime.now(datetime.timezone.utc),
        "endpoint": endpoint,
        "params": params,
        "is_valid": response.status_code == 200,
        "response": response.json(),
    }

    dset_table_name = "dset_responses"

    dset_table = get_response_table(dset_table_name, schema=schema)

    insert_statement = (
        insert(dset_table)
        .values(request_data)
        .returning(
            dset_table.c.is_valid,
            dset_table.c.response,
        )
    )
    result = conn.execute(insert_statement)

    return [dict(r) for r in result.all()]


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
        {
            **request_meta,
            **flat_readings_meta,
            **reading,
        }
        for reading in flat_readings
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

    return [store_dset(conn, group, schema) for group in readings]


def localize_time_range(from_ts: datetime.datetime, to_ts: datetime.datetime):
    dset_timezone = pytz.timezone("Europe/Madrid")
    # pendulum is better, astimezone on naïf assumes system's timezone
    if from_ts.tzinfo == None:
        from_ts_local = dset_timezone.localize(from_ts)
        to_ts_local = dset_timezone.localize(to_ts)
    else:
        from_ts_local = from_ts.astimezone(dset_timezone)
        to_ts_local = to_ts.astimezone(dset_timezone)
    return from_ts_local, to_ts_local


def get_dset_to_db(conn, endpoint, apikey, queryparams, schema):
    response = httpx.get(
        endpoint,
        params=queryparams,
        headers={"Authorization": apikey},
    )

    response.raise_for_status()

    return store_dset_response(conn, response, endpoint, queryparams, schema)
