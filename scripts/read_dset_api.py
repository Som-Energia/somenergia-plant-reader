import logging
import typer
from sqlalchemy import create_engine

from plant_reader import get_config, read_dset, read_store_dset
from plant_reader.dset_reader import create_table

logger = logging.getLogger(__name__)

app = typer.Typer()


@app.command()
def setupdb(
    dbapi: str,
    table: str,
    schema: str,
):
    dbapi = get_config(dbapi)

    db_engine = create_engine(dbapi)
    with db_engine.begin() as conn:
        create_table(conn, table, schema=schema)


@app.command()
def print_readings(base_url: str, apikey: str):
    logging.info(f"Reading {base_url}")

    registries = read_dset(base_url, apikey)

    logging.info(f"Read registries\n:{registries}\n")

    return 0


@app.command()
def get_readings(
    dbapi: str,
    base_url: str,
    apikey: str,
    schema: str = typer.Option("public", "--schema"),
):
    db_engine = create_engine(dbapi)
    with db_engine.begin() as conn:
        logging.info(f"Reading {base_url}")
        readings = read_store_dset(conn, base_url, apikey, schema)
        logging.info(readings)

    return 0


if __name__ == "__main__":
    app()

# vim: et sw=4 ts=4
