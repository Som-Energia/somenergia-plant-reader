import datetime
import logging
import typing as T
from json import JSONDecodeError

import httpx
import numpy as np
import pandas as pd
import pytz
import sqlalchemy as sa
import typer
from httpx import Timeout

TABLE_NAME__DSET_METERS_READINGS = "dset_meters_readings"


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)

logger = logging.getLogger(__name__)
app = typer.Typer()


@app.command()
def get_historic_readings_meters(
    dbapi: str = typer.Option(..., "--db-url"),
    base_url: str = typer.Option(
        "https://api.dset-energy.com",
        "--api-base-url",
        help="Base URL of the DSET API",
    ),
    apikey: str = typer.Option(..., "--api-key"),
    schema: str = typer.Option(..., "--schema"),
    request_time_offset_min: int = typer.Option(
        30,
        "--request-time-offset-min",
        help="Minutes to wait before requesting the data",
    ),
    query_timeout: float = typer.Option(
        10.0, "--query-timeout", help="Query timeout in seconds"
    ),
    apply_k_value: bool = typer.Option(
        False,
        "--apply-k-value",
        help="Apply dset k value transformations factor",
        is_flag=True,
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Don't commit to db", is_flag=True
    ),
):
    """Get historic readings from the DSET API, compare with what we have at schema and commit them to the database"""

    queryparams = {}

    logger.info("Fetching groups from the DSET API")

    response = httpx.get(
        base_url + "/api/groups",
        params=queryparams,
        headers={"Authorization": apikey},
        timeout=Timeout(timeout=query_timeout),
    )

    response.raise_for_status()

    filter_frequency = "15 minutes"
    filtered_signals = __transform_group_response(
        response.json(),
        filter_frequency,
    )

    logger.info(
        f"{len(filtered_signals)} signals left after"
        f" filtering by frequency {filter_frequency}"
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
    db_engine = sa.create_engine(dbapi)

    # assess if table exists
    insp = sa.inspect(db_engine)
    table_exists = insp.has_table(
        TABLE_NAME__DSET_METERS_READINGS,
        schema=schema,
    )

    if not table_exists:

        # we update fields to match the lake table
        df_last_signals["ts"] = df_last_signals["signal_last_ts"]
        df_last_signals["signal_value"] = df_last_signals["signal_last_value"]

        logger.info(
            f"Table {schema}.{TABLE_NAME__DSET_METERS_READINGS}"
            " does not exist, creating it as we insert the data"
        )

        df_last_signals.to_sql(
            con=db_engine,
            name=TABLE_NAME__DSET_METERS_READINGS,
            schema=schema,
            if_exists="fail",
            index=False,
        )
    else:
        # compare last reading and id with stored readings
        logger.info("Table exists, comparing last readings with stored ones")

        df_lake_meters = _get_lake_latest_signals(schema, db_engine)

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
            "_merge == 'both'"
            " and signal_last_ts > max_last_ts"
            " or _merge == 'left_only'"
        ).replace({np.nan: None})

        logger.info("Found {len(df_in_lake_outdated)} signals that needs updating")

        for ix, signal in df_in_lake_outdated.iterrows():

            # we craft a request to fetch data from
            signal_id = signal["signal_id"]

            if signal["_merge"] == "both":
                date_from = signal["max_last_ts"]
            else:
                date_from = signal["signal_last_ts"]

            date_to = datetime.datetime.now(tz=pytz.timezone("Europe/Madrid"))

            logger.info(f"Updating signal {signal_id} from {date_from} to {date_to}")

            params = {
                "from": date_from.strftime("%Y-%m-%d"),
                "to": date_to.strftime("%Y-%m-%d"),
                "applykvalue": apply_k_value,
            }

            logger.info(f"Querying data for signal with params: {params}")

            response = _query_data_latest(
                signal_id=signal_id,
                api_key=apikey,
                query_params=params,
                query_timeout=query_timeout,
                base_url=base_url,
            )

            response.raise_for_status()

            df_response = pd.DataFrame(response.json())

            df_new_signals = _extend_response(df_response, signal)

            logger.info(f"{len(df_new_signals)} new readings fetched")

            if not dry_run:
                df_new_signals.to_sql(
                    con=db_engine,
                    name=TABLE_NAME__DSET_METERS_READINGS,
                    schema=schema,
                    if_exists="append",
                    index=False,
                )

            else:
                logger.info("Dry run, not committing to the database")


def _extend_response(
    df_response: pd.DataFrame,
    signal: dict,
) -> pd.DataFrame:
    """Extend the response from /api/data/{signal_id} so that it matches the schema of the lake table

    The response from the DSET API is a list of dictionaries with the following fields:

    [
        {
            "ts": "2021-01-01 00:00:00",
            "value": 77
        }
    ]

    Whereas the lake table has the shape of the output of __transform_group_response:

    [
        {
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
            "signal_unit": "kWh"
            "ts": "2021-01-01 00:00:00", # new from the response
            "signal_value": 77 # new from the response
        }
    ]
    """
    df_response.rename(columns={"value": "signal_value"}, inplace=True)

    df_response["group_id"] = signal["group_id"]
    df_response["group_code"] = signal["group_code"]
    df_response["signal_id"] = signal["signal_id"]
    df_response["signal_code"] = signal["signal_code"]
    df_response["signal_description"] = signal["signal_description"]
    df_response["signal_frequency"] = signal["signal_frequency"]
    df_response["signal_type"] = signal["signal_type"]
    df_response["signal_is_virtual"] = signal["signal_is_virtual"]
    df_response["signal_tz"] = signal["signal_tz"]
    df_response["signal_unit"] = signal["signal_unit"]
    df_response["signal_last_ts"] = signal["max_last_ts"]
    df_response["signal_last_value"] = signal["max_last_ts_value"]

    return df_response


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


def _get_lake_latest_signals(schema: str, db_engine: sa.engine.Engine) -> pd.DataFrame:

    df = pd.read_sql(
        f"""
            SELECT
                signal_id,
                signal_value as max_last_ts_value,
                max(ts) OVER (PARTITION BY signal_id) AS max_last_ts
            FROM plants.{schema}.{TABLE_NAME__DSET_METERS_READINGS}
            """,
        db_engine,
        parse_dates=["max_last_ts"],
    ).astype(
        {
            "signal_id": "int64",
            "max_last_ts_value": "float64",
            "max_last_ts": "datetime64[ns, Europe/Madrid]",
        }
    )

    return df


def _query_data_latest(
    signal_id: str,
    api_key: str,
    query_params: dict,
    query_timeout: float,
    base_url: str,
) -> httpx.Response:
    """Query the latest data from the DSET API using the signal id"""

    response = httpx.get(
        url=base_url + f"/api/data/export/{signal_id}",
        params=query_params,
        headers={"Authorization": api_key},
        timeout=Timeout(timeout=query_timeout),
    )

    response.raise_for_status()
    return response


if __name__ == "__main__":
    app()
