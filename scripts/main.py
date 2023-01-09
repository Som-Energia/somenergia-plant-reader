import logging
from typing import List, Tuple
import typer
from sqlalchemy import create_engine

from plant_reader import get_config
from plant_reader import main_read_store, create_table, read_modbus

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

app = typer.Typer()

@app.command()
def setupdb(dbapi: str, table: str):
    dbapi = get_config(dbapi)

    db_engine = create_engine(dbapi)
    with db_engine.begin() as conn:
       create_table(conn, table)

@app.command()
def get_readings(
        dbapi: str,
        table: str,
        ip: str,
        port: int,
        type: str,
        modbus_tuple: List[str]
    ):

    dbapi = get_config(dbapi)

    logging.debug(f"Connecting to DB")

    if not modbus_tuple:
        print("No provided modbus tuples. Expected --modbus-tuple unit address count")
        raise typer.Abort()

    modbus_tuples = [tuple(int(e) for e in mt.split(':')) for mt in modbus_tuple]

    db_engine = create_engine(dbapi)
    with db_engine.begin() as conn:
        main_read_store(conn, table, ip, port, type, modbus_tuples)

    return 0


@app.command()
def print_multiple_readings(
        ip: str,
        port: int,
        type: str,
        modbus_tuple: List[str]
    ):


    print("Reading modbus")

    modbus_tuples = [tuple(int(e) for e in mt.split(':')) for mt in modbus_tuple]

    print(modbus_tuples)

    for mt in modbus_tuples:
        slave,register_address, count = mt
        print(f'Read modbus tuple: {slave} {register_address} {count}')

        registries = read_modbus(ip, port, type, register_address, count, slave)

        print(f'Read registries\n:{registries}\n')


    return 0

@app.command()
def print_readings(
        ip: str,
        port: int,
        slave: int,
        type: str,
        register_address: int,
        count: int
    ):


    print("Reading modbus")

    registries = read_modbus(ip, port, type, register_address, count, slave)

    print(f'Read registries\n:{registries}\n')


    return 0


if __name__ == '__main__':
  app()

# vim: et sw=4 ts=4