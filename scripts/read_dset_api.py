import pandas as pd
import numpy as np
import datetime
import logging
import typing as T
from json import JSONDecodeError

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
def get_historic_readings_meters(
    dbapi: str = typer.Option(..., "--db-url"),
    base_url: str = typer.Option("https://api.dset-energy.com", "--api-base-url"),
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
):
    # hack to make it not inclusive...
    to_date = to_date - datetime.timedelta(seconds=1)

    # temporary workaround to account for the delay of the remote api, see https://gitlab.somenergia.coop/et/somenergia-plant-reader/-/issues/2
    # might be counter-intuitive since the time range of the airflow interval will not be the actually run

    wait_delta = datetime.timedelta(minutes=request_time_offset_min)
    from_date = from_date - wait_delta
    to_date = to_date - wait_delta

    queryparams = {}
    response = httpx.get(
        base_url + "/api/groups",
        params=queryparams,
        headers={"Authorization": apikey},
        timeout=Timeout(timeout=query_timeout),
    )

    response.raise_for_status()

    def __transform_group_response(
        group_data: "list[dict]",
        frequency: str = "15 minutes",
    ) -> "list[str]":
        """Get a modified object from group data based on their reported frequency

        This will flatten the signals and group data into a list of dictionaries
        filtered by the frequency reported at a signal level.

        >>> group_response_json = [
            {
                "group_id": 1,
                "group_code": "a",
                "group_name": "b",
                "signals": [
                    {
                        "signal_id": 12,
                        "signal_code": "some_code",
                        "signal_description": "some_description",
                        "signal_frequency": "15 minutes",
                        "signal_type": "absolute",
                        "signal_is_virtual": False,
                        "signal_tz": "Europe/Madrid",
                        "signal_last_ts": "2024-02-13 23:45:00",
                        "signal_last_value": 0,
                        "signal_unit": "kWh",
                    },
                    {
                        "signal_id": 11,
                        "signal_code": "some_other_code",
                        "signal_description": "some_other_description",
                        "signal_frequency": "2222 minutes", # not the same frequency
                        "signal_type": "absolute",
                        "signal_is_virtual": False,
                        "signal_tz": "Europe/Madrid",
                        "signal_last_ts": "2024-02-13 21:45:00",
                        "signal_last_value": 10,
                        "signal_unit": "kWh",
                    },
                ],
            },
        ]

        >>> __transform_group_response(group_response_json, "15 minutes")
        [{
            "group_id": 1,
            "group_code": "a",
            "group_name": "b",
            "signal_id": 12,
            "signal_code": "some_code",
            "signal_description": "some_description",
            "signal_frequency": "15 minutes",
            "signal_type": "absolute",
            "signal_is_virtual": False,
            "signal_tz": "Europe/Madrid",
            "signal_last_ts": "2024-02-13 23:45:00",
            "signal_last_value": 0,
            "signal_unit": "kWh",
        }]
        """

        return [
            {
                "group_id": g["group_id"],
                "group_code": g["group_code"],
                "group_name": g["group_name"],
                **sig,
            }
            for g in group_data
            for sig in g.get("signals", [])
            if sig["signal_frequency"] == frequency
        ]

    filter_frequency = "15 minutes"
    filtered_signals = __transform_group_response(
        response.json(),
        filter_frequency,
    )

    logger.info(
        f"Filtered signals with frequency {filter_frequency}: {len(filtered_signals)}"
    )

    _signals_dtype = {
        "group_id": "int64",
        "group_code": "string",
        "group_name": "string",
        "signal_id": "int64",
        "signal_code": "string",
        "signal_description": "string",
        "signal_frequency": "string",
        "signal_type": "string",
        "signal_is_virtual": "boolean",
        "signal_tz": "string",
        "signal_last_ts": "datetime64[ns, Europe/Madrid]",
        "signal_last_value": "float64",
        "signal_unit": "string",
    }

    df_last_signals = pd.DataFrame(filtered_signals).astype(_signals_dtype)

    db_engine = create_engine(dbapi)

    # compare last reading and id with stored readings
    df_lake_meters = pd.read_sql(
        """
        SELECT
            signal_id,
            max(last_ts) OVER (PARTITION BY signal_id) AS max_last_ts
        FROM plants.lake.dset_meters_readings
        """,
        db_engine,
        parse_dates=["max_last_ts"],
    ).astype(
        {
            "signal_id": "int64",
            "max_last_ts": "datetime64[ns, Europe/Madrid]",
        }
    )

    # full outer join to get the last readings and the last stored readings
    df_recent = df_last_signals.merge(
        df_lake_meters,
        left_on="signal_id",
        right_on="signal_id",
        how="outer",
        indicator=True,
        validate="one_to_one",
        suffixes=("__dset", "__som"),
    )

    # filter the ones that are not in the lake
    df_in_lake_outdated = df_recent.query(
        "_merge == 'both' and last_ts > max_last_ts or _merge == 'left_only'"
    )

    for ix, signal in df_in_lake_outdated.replace({np.nan: None}).iterrows():

        signal_id = signal["signal_id"].item()
        date_from = signal["max_last_ts"].item() or signal["last_ts"].item()
        date_to = datetime.datetime.now(tz="Europe/Madrid")

        logger.info(f"Updating signal {signal_id} from {date_from} to {date_to}")

        params = {
            "from": date_from.strftime("%Y-%m-%d"),
            "to": date_to.strftime("%Y-%m-%d"),
            "applykvalue": apply_k_value,
        }

        response = httpx.get(
            url=base_url + f"/api/data/export/{signal_id}",
            params=params,
            headers={"Authorization": apikey},
            timeout=Timeout(timeout=query_timeout),
        )

        response.raise_for_status()

        # craft response
        df_response = pd.DataFrame(response.json())
        df_response["signal_id"] = signal_id
        df_response["last_ts"] = pd.to_datetime(date_to, utc=True)
        df_response["group_id"]

        if not dry_run:
            stored = ...
            logger.info(f"{len(stored)} readings stored")
            logger.info(stored)
        else:
            logger.info(f"{len(stored)} readings stored")
            logger.info(stored)


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
):
    # hack to make it not inclusive...
    to_date = to_date - datetime.timedelta(seconds=1)

    # temporary workaround to account for the delay of the remote api, see https://gitlab.somenergia.coop/et/somenergia-plant-reader/-/issues/2
    # might be counter-intuitive since the time range of the airflow interval will not be the actually run

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

    db_engine = create_engine(dbapi)
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

        if not dry_run:
            stored = store_dset_response(conn, response, endpoint, queryparams, schema)
            logging.info(f"{len(stored)} readings stored")
            logging.info(stored)
        else:
            response_json = response.json()
            logging.info(
                "Readings retrieved" if response_json else "No readings retrieved"
            )
            logging.info(response_json)

    return 0


if __name__ == "__main__":
    app()

# vim: et sw=4 ts=4
