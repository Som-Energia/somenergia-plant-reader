import datetime
import logging
import typing as T

import httpx
import sqlalchemy as sa
import typer
from httpx import Timeout

from plant_reader import get_config, read_dset, read_store_dset
from plant_reader.dset_reader import (
    create_response_table,
    create_table,
    store_dset_response,
)

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
    config = get_config(environment=dbapi)

    db_engine = sa.create_engine(config.db_url)
    with db_engine.begin() as conn:
        create_table(conn, table, schema=schema)


@app.command()
def create_responses_table(
    dbapi: str,
    schema: str,
):
    table = "dset_responses"

    config = get_config(environment=dbapi)
    db_engine = sa.create_engine(config.db_url)

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
    db_engine = sa.create_engine(dbapi)
    with db_engine.begin() as conn:
        logger.info(f"Reading {base_url}")
        readings = read_store_dset(conn, base_url, apikey, schema)
        logger.info(readings)

    return 0


@app.command()
def get_historic_readings(
    dbapi: str = typer.Option(..., "--db-url"),
    base_url: str = typer.Option(
        ..., "--api-base-url", help="Base URL of the DSET api"
    ),
    apikey: str = typer.Option(..., "--api-key"),
    schema: str = typer.Option(..., "--schema", help="Schema to store the readings"),
    endpoint: str = typer.Option(..., "--endpoint", help="Endpoint to request"),
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
    request_time_offset_min: int = typer.Option(
        30,
        "--request-time-offset-min",
        help="Minutes to wait before requesting the data",
    ),
    query_timeout: float = typer.Option(
        10.0, "--query-timeout", help="Query timeout in seconds"
    ),
    sig_detail: bool = typer.Option(
        False, "--sig-detail", help="Provide extra fields", is_flag=True
    ),
    apply_k_value: bool = typer.Option(
        False,
        "--apply-k-value",
        help="Apply dset k value transformations factor",
        is_flag=True,
    ),
    return_null_values: bool = typer.Option(
        False, "--return-null-values", help="Put null on missing values", is_flag=True
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Don't commit to db", is_flag=True
    ),
    validate_response: bool = typer.Option(
        False,
        "--validate-response",
        help="Validate response",
        is_flag=True,
    ),
):
    # hack to make it not inclusive...
    to_date = to_date - datetime.timedelta(seconds=1)

    # temporary workaround to account for the delay of the remote api,
    # see https://gitlab.somenergia.coop/et/somenergia-plant-reader/-/issues/2
    # might be counter-intuitive since the time range of the airflow interval
    # will not be the actually run

    wait_delta = datetime.timedelta(minutes=request_time_offset_min)
    from_date = from_date - wait_delta
    to_date = to_date - wait_delta

    queryparams = {
        "from": from_date.isoformat(),
        "to": to_date.isoformat(),
        "sig_detail": sig_detail,
        "applykvalue": apply_k_value,
        "returnNullValues": return_null_values,
    }

    db_engine = sa.create_engine(dbapi)
    full_url = base_url + endpoint

    with db_engine.begin() as conn:
        logging.info((f"Reading {base_url} from {from_date} to {to_date}"))

        response = httpx.get(
            full_url,
            params=queryparams,
            headers={"Authorization": apikey},
            timeout=Timeout(timeout=query_timeout),
        )

        response.raise_for_status()
        response_json = response.json()

        if validate_response and not _validate_non_null_data_or_ts(response_json):
            print("Response is not valid, exiting.")
            raise typer.Exit(code=1)

        if not dry_run:
            stored = store_dset_response(conn, response, endpoint, queryparams, schema)
            logging.info("%s readings stored", len(stored))
            logging.info(stored)
        else:
            logging.info(
                "Readings retrieved" if response_json else "No readings retrieved"
            )
            logging.info(response_json)

    return 0


def _validate_non_null_data_or_ts(response_json: T.Dict) -> bool:
    """Validate that the response has non-null data or timestamps

    Parameters
    ----------
    response_json : dict, optional
        A dictionary with the response from the DSET API

    Returns
    -------
    bool
        Whether the response is valid. A valid response is one that has
        the signals field, and inside each one of them has the fields
        {"data": {"ts": VALUE, "value": VALUE}}

        If the fields are not present, the response is considered invalid
        and this will return False. If partial data is present, it will
        log a warning and return True.
    """

    import pandas as pd

    df = pd.DataFrame(response_json)

    # -------------------------- validate signals field -------------------------- #

    if "signals" not in df.columns:
        logger.error("No 'signals' field found in the response")
        return False

    signals_null = df["signals"].explode().isnull().sum()

    if signals_null > 0:
        logger.warning(
            f"Found {signals_null} rows with null values in the 'signals' column"
        )

    df_signals = (
        df["signals"]  # extract only the signals column
        .explode()  # turn json arrays into rows
        .apply(pd.Series, dtype="object")  # turn the json into columns
        .dropna(axis=1, how="all")  # drop columns with all nulls
        .dropna(axis=0, how="all")  # drop rows with all nulls
    )

    # ---------------------------- validate data field --------------------------- #

    if "data" not in df_signals.columns:
        logger.error("No 'data' field found inside the 'signals' field")
        return False

    rows_data_null = df_signals["data"].explode().isnull().sum()

    if rows_data_null > 0:
        logger.warning(
            f"Found {rows_data_null} rows with null values in the 'data' column"
        )

    # ----------------------- validate ts and value columns ---------------------- #

    df_data = df_signals["data"].explode().apply(pd.Series, dtype="object")

    if "ts" not in df_data.columns or "value" not in df_data.columns:
        logger.error(
            "No 'ts' or 'value' field found inside the 'data' field for this query."
        )
        return False

    rows_ts_value_null = df_data.isna().sum().to_dict()
    rows_total = df_data.shape[0]

    if rows_ts_value_null["ts"] > 0:
        logger.warning(
            "Found %s rows (%.2f%%) with null values in the 'ts' column",
            rows_ts_value_null["ts"],
            (rows_ts_value_null["ts"] / rows_total) * 100,
        )

    if rows_ts_value_null["value"] > 0:
        logger.warning(
            "Found %s rows (%.2f%%) with null values in the 'value' column",
            rows_ts_value_null["value"],
            (rows_ts_value_null["value"] / rows_total) * 100,
        )

    return True


if __name__ == "__main__":
    app()

# vim: et sw=4 ts=4
