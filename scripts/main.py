import logging
from typing import List

import typer
from sqlalchemy import create_engine

from plant_reader import get_config, main_read_store, read_modbus
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
    dbapi: str = typer.Option(..., "--dbapi", help="SQLAlchemy url of the db to use"),
    table: str = typer.Option(..., "--table", help="Table to store readings"),
    ip: str = typer.Option(..., "--ip", help="IP address of modbus device"),
    port: int = typer.Option(..., "--port", help="Port of modbus device"),
    type: str = typer.Option(..., "--type", help="Type of modbus device"),
    modbus_tuple: List[str] = typer.Option(
        ...,
        "--modbus-tuple",
        help=(
            "Tuple describing a modbus mapping in the form <unit>:<address>:<count>."
            " Can be repeated multiple times."
        ),
        callback=lambda mts: [tuple(map(int, mt.split(":"))) for mt in mts],
    ),
    schema: str = typer.Option("public", "--schema"),
):
    logger.debug("Connecting to DB")

    if not modbus_tuple:
        logger.error(
            "No provided modbus tuples. Expected --modbus-tuple unit address count"
        )
        raise typer.Abort()

    db_engine = create_engine(dbapi)

    with db_engine.begin() as conn:
        main_read_store(conn, table, ip, port, type, modbus_tuple, schema)

    return 0


@app.command()
def print_multiple_readings(
    ip: str = typer.Option(..., "--ip", help="IP address of modbus device"),
    port: int = typer.Option(..., "--port", help="Port of modbus device"),
    type: str = typer.Option(..., "--type", help="Type of modbus device"),
    modbus_tuple: List[str] = typer.Option(
        ...,
        "--modbus-tuple",
        help=(
            "Modbus tuple in the form <unit>:<address>:<count>."
            " Can be repeated multiple times."
        ),
        callback=lambda mts: [tuple(map(int, mt.split(":"))) for mt in mts],
    ),
):
    logger.info("Reading modbus")

    logger.info(modbus_tuple)

    for mt in modbus_tuple:
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
