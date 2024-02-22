import logging
from typing import List, Tuple
import typer
from sqlalchemy import create_engine

from plant_reader import get_config
from plant_reader import main_read_store, read_modbus
from plant_reader.modbus_reader import create_table

logger = logging.getLogger(__name__)

app = typer.Typer()


@app.command()
def setupdb(
    dbapi: str,
    table: str,
    schema: str,
):

    config = get_config(environment=dbapi)
    db_engine = create_engine(config.db_url)
    with db_engine.begin() as conn:
        create_table(conn, table, schema=schema)


@app.command()
def get_readings(
    dbapi: str,
    table: str,
    ip: str,
    port: int,
    type: str,
    modbus_tuple: List[str],
    schema: str = typer.Option("public", "--schema"),
):
    config = get_config(environment=dbapi)

    logger.debug("Connecting to DB")

    if not modbus_tuple:
        logger.error(
            "No provided modbus tuples. Expected --modbus-tuple unit address count"
        )
        raise typer.Abort()

    modbus_tuples = [tuple(int(e) for e in mt.split(":")) for mt in modbus_tuple]

    db_engine = create_engine(config.db_url)

    with db_engine.begin() as conn:
        main_read_store(conn, table, ip, port, type, modbus_tuples, schema)

    return 0


@app.command()
def print_multiple_readings(ip: str, port: int, type: str, modbus_tuple: List[str]):
    logger.info("Reading modbus")

    modbus_tuples = [tuple(int(e) for e in mt.split(":")) for mt in modbus_tuple]

    logger.info(modbus_tuples)

    for mt in modbus_tuples:
        slave, register_address, count = mt
        logger.info(f"Read modbus tuple: {slave} {register_address} {count}")

        registries = read_modbus(ip, port, type, register_address, count, slave)

        logger.info(f"Read registries\n:{registries}\n")

    return 0


@app.command()
def print_readings(
    ip: str, port: int, slave: int, type: str, register_address: int, count: int
):
    logger.info("Reading modbus")

    registries = read_modbus(ip, port, type, register_address, count, slave)

    logger.info(f"Read registries\n:{registries}\n")

    return 0


if __name__ == "__main__":
    app()

# vim: et sw=4 ts=4
