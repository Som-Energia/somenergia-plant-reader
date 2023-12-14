import datetime
import logging
import typing as T

import httpx
import typer
from httpx import Timeout
from sqlalchemy import create_engine

from plant_reader import (
    get_config,
    get_dset_to_db,
    localize_time_range,
    read_dset,
    read_store_dset,
)
from plant_reader.dset_reader import create_response_table, create_table

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
    dbapi: str = typer.Option(..., "--db-url"),
    base_url: str = typer.Option(..., "--api-base-url"),
    apikey: str = typer.Option(..., "--api-key"),
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
    dbapi: str = typer.Option(..., "--db-url"),
    base_url: str = typer.Option(..., "--api-base-url"),
    apikey: str = typer.Option(..., "--api-key"),
    schema: str = typer.Option(..., "--schema"),
    endpoint: str = typer.Option("/api/data", "--endpoint"),
    from_date: datetime.datetime = typer.Option(
        ...,
        "--from-date",
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
        "--to-date",
        help="timestamp with timezone, not inclusive.",
        formats=[
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%d %H:%M:%S%z",
            "%Y-%m-%d",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d %H:%M:%S",
        ],
    ),
    wait_min_before_request: int = typer.Option(
        30,
        "--wait-min-before-request",
        help="Minutes to wait before requesting the data",
    ),
):
    # hack to make it not inclusive...
    to_date = to_date - datetime.timedelta(seconds=1)

    # temporary workaround to account for the delay of the remote api, see https://gitlab.somenergia.coop/et/somenergia-plant-reader/-/issues/2
    # might be counter-intuitive since the time range of the airflow interval will not be the actually run

    wait_delta = datetime.timedelta(minutes=wait_min_before_request)
    from_date = from_date - wait_delta
    to_date = to_date - wait_delta

    from_date_local, to_date_local = localize_time_range(from_date, to_date)

    queryparams = {
        "from": from_date_local.isoformat(),
        "to": to_date_local.isoformat(),
        "sig_detail": True,
    }

    db_engine = create_engine(dbapi)
    full_url = base_url + endpoint

    with db_engine.begin() as conn:
        logging.info(
            (
                f"Reading {base_url}"
                f" from {from_date_local}"
                f" to {to_date_local} (local times)"
            )
        )

        readings = get_dset_to_db(
            conn,
            full_url,
            apikey,
            queryparams,
            schema,
        )

        logging.info("Readings retrieved" if readings else "No readings retrieved")
        logging.info(readings)

    return 0


if __name__ == "__main__":
    app()

# vim: et sw=4 ts=4
