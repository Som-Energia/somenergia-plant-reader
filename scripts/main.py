import logging
from typing import List
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
        slave: int,
        type: str,
        register_address_count: List[int]
    ):

    dbapi = get_config(dbapi)

    logging.debug(f"Connecting to DB")

    db_engine = create_engine(dbapi)
    with db_engine.begin() as conn:
        main_read_store(conn, table, ip, port, type, register_address_count, slave)

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