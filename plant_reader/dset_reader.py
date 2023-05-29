
import httpx
import datetime
from sqlalchemy import table, column, MetaData, DateTime, Boolean, Column, String, Table
from sqlalchemy.dialects.postgresql import JSONB, insert
import json


def create_table(conn, table_name):

    meta = MetaData(conn)
    dbtable = Table(table_name, meta,
        Column("query_time", DateTime(timezone=True)),
        Column("endpoint", String),
        Column("params",JSONB),
        Column("is_valid", Boolean),
        Column("data", JSONB)
    )

    dbtable.create(conn, checkfirst=True)

def get_table(table_name):

    return table(
        table_name,
        column("query_time", DateTime(timezone=True)),
        column("endpoint", String),
        column("params",JSONB),
        column("is_valid", Boolean),
        column("data", JSONB)
    )

def read_dset(base_url, apikey):

    print(f'keys: {apikey}')

    url = f'{base_url}/api/groups'

    response = httpx.get(url, headers={'Authorization':apikey})

    response.raise_for_status()

    return response.json()[0]

def read_store_dset(conn, base_url, apikey):

    dset_table = get_table('dset')

    print(f'keys: {apikey}')

    readings = read_dset(base_url, apikey)

    # TODO quick and dirty
    json_readings = json.dumps(readings, indent=4, sort_keys=True, default=str)

    reading_data = {'query_time': datetime.datetime.now(datetime.timezone.utc), 'endpoint': '/api/groups',  'params': {}, 'is_valid': True, 'data':json_readings}

    insert_statement = insert(dset_table).values(**reading_data)
    result = conn.execute(insert_statement)

    return result