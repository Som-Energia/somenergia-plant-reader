import logging
import typer
from sqlalchemy import create_engine
import datetime

from plant_reader import get_config, create_table, read_dset, read_store_dset

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

app = typer.Typer()


@app.command()
def setupdb(dbapi: str, table: str):
    dbapi = get_config(dbapi)

    db_engine = create_engine(dbapi)
    with db_engine.begin() as conn:
       create_table(conn, table)

@app.command()
def print_readings(
        base_url: str,
        apikey: str
    ):


    logging.info(f"Reading {base_url}")

    registries = read_dset(base_url, apikey)

    logging.info(f'Read registries\n:{registries}\n')

    return 0

@app.command()
def get_readings(
        dbapi: str,
        base_url: str,
        apikey: str
    ):

    db_engine = create_engine(dbapi)
    with db_engine.begin() as conn:
        logging.info(f"Reading {base_url}")
        logging.info(f"The apikey is {apikey}")
        readings = read_store_dset(conn, base_url, apikey)
        logging.info(readings)

    return 0



if __name__ == '__main__':
  app()

# vim: et sw=4 ts=4