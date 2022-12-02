import logging
import typer
from sqlalchemy import create_engine

from plant_reader import get_config
from plant_reader import main_function

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

app = typer.Typer()

@app.command()
def main(
        dbapi: str
    ):

    dbapi = get_config(dbapi)

    logging.debug(f"Connecting to DB")

    db_engine = create_engine(dbapi)
    with db_engine.begin() as conn:
        main_function(conn)

    return


if __name__ == '__main__':
  app()

# vim: et sw=4 ts=4