import logging
import typer
import datetime
from sqlalchemy import create_engine

from plant_reader import (
    get_config,
    read_dset,
    read_store_dset,
    get_dset_to_db,
    localize_time_range,
)
from plant_reader.dset_reader import create_table, create_response_table

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)

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
def create_responses_table(
    dbapi: str,
    schema: str,
):
    table = "dset_responses"
    dbapi = get_config(dbapi)

    db_engine = create_engine(dbapi)
    with db_engine.begin() as conn:
        create_response_table(conn, table, schema=schema)


@app.command()
def print_readings(base_url: str, apikey: str):
    logger.info(f"Reading {base_url}")

    registries = read_dset(base_url, apikey)

    logger.info(f"Read registries\n:{registries}\n")

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
        logger.info(f"Reading {base_url}")
        readings = read_store_dset(conn, base_url, apikey, schema)
        logger.info(readings)

    return 0


@app.command()
def get_historic_readings(
    dbapi: str,
    base_url: str,
    apikey: str,
    from_date: datetime.datetime = typer.Option(
        ...,
        help="timestamp with timezone, inclusive.",
        formats=[
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%d %H:%M:%S%z",
            "%Y-%m-%d",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d %H:%M:%S",
        ],
    ),
    to_date: datetime.datetime = typer.Option(
        ...,
        help="timestamp with timezone, not inclusive.",
        formats=[
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%d %H:%M:%S%z",
            "%Y-%m-%d",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d %H:%M:%S",
        ],
    ),
    schema: str = typer.Option("public", "--schema"),
):
    # hack to make it not inclusive...
    to_date = to_date - datetime.timedelta(seconds=1)

    # TODO ! temporary workaround to account for the delay of the remote api
    # might be counter-intuitive since the time range of the airflow interval will not be the actually run
    from_date = from_date - datetime.timedelta(minutes=15)
    to_date = to_date - datetime.timedelta(minutes=15)

    from_date_local, to_date_local = localize_time_range(from_date, to_date)

    queryparams = {
        "from": from_date_local.isoformat(),
        "to": to_date_local.isoformat(),
        "sig_detail": True,
    }

    db_engine = create_engine(dbapi)

    with db_engine.begin() as conn:
        logging.info(
            f"Reading {base_url} from {from_date_local} to {to_date_local} (local times)"
        )
        readings = get_dset_to_db(
            conn,
            base_url,
            apikey,
            queryparams,
            schema,
        )

        logging.info("Readings retrieved" if readings else "No readings retrieved")
        logging.info(f"{readings.keys()=}")
        logging.debug(readings)

    return 0


if __name__ == "__main__":
    app()

# vim: et sw=4 ts=4
