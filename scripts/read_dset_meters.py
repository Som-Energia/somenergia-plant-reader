import datetime
import logging
import typing as T

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
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Don't commit to db", is_flag=True
    ),
):
    """Get historic readings from the DSET API and compare with what we have"""

    queryparams = {
        "sig_detail": sig_detail,
        "applykvalue": apply_k_value,
    }

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
        "signal_external_id": "string",
        "signal_device_external_description": "string",
        "signal_device_external_id": "string",
    }

    _signals_sql_types = {
        "group_id": sa.Integer,
        "group_code": sa.String,
        "group_name": sa.String,
        "signal_id": sa.Integer,
        "signal_code": sa.String,
        "signal_description": sa.String,
        "signal_frequency": sa.String,
        "signal_type": sa.String,
        "signal_is_virtual": sa.Boolean,
        "signal_tz": sa.String,
        "signal_last_ts": sa.DateTime(timezone=True),
        "signal_last_value": sa.Float,
        "signal_unit": sa.String,
        "signal_external_id": sa.String,
        "signal_device_external_description": sa.String,
        "signal_device_external_id": sa.String,
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
        df_last_signals = __extend_df_first_insert(df_last_signals)

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
            dtype=_signals_sql_types,
        )
    else:
        logger.info("Table exists, comparing last readings with stored ones")

        df_lake_meters = __get_lake_latest_signals(schema, db_engine)

        df_in_lake_outdated = __resolve_outdated_signals(
            df_api=df_last_signals,
            df_lake=df_lake_meters,
        )

        logger.info(f"Found {len(df_in_lake_outdated)} signals that needs updating")

        for _, signal in df_in_lake_outdated.iterrows():
            __append_new_signal_in_db(
                signal,
                api_key=apikey,
                base_url=base_url,
                apply_k_value=apply_k_value,
                sig_detail=sig_detail,
                engine=db_engine,
                schema=schema,
                query_timeout=query_timeout,
                dry_run=dry_run,
            )


def __extend_df_first_insert(df_last_signals):
    """Extends a dataframe to match the lake table in the first insert."""

    df_last_signals["ts"] = df_last_signals["signal_last_ts"]
    df_last_signals["signal_value"] = df_last_signals["signal_last_value"]

    return df_last_signals


def __resolve_outdated_signals(
    df_api: pd.DataFrame,
    df_lake: pd.DataFrame,
) -> pd.DataFrame:
    """Filter outdated signals comparing data from api with data in lake

    Performs a full outer join to get the last readings
    and the last stored readings"""

    df_recent = df_api.merge(
        df_lake,
        left_on="signal_id",
        right_on="signal_id",
        how="outer",
        indicator=True,
        # validate="one_to_one", # raises when they dont match
        suffixes=("__dset", "__som"),
    )

    df_in_lake_outdated = df_recent.query(
        "_merge == 'both'"
        " and signal_last_ts > max_last_ts"
        " or _merge == 'left_only'"
    ).replace({np.nan: None})

    return df_in_lake_outdated


def __extend_response(
    df_response: pd.DataFrame,
    signal: dict,
) -> pd.DataFrame:
    """Extend the response from /api/data/{signal_id} to match the lake

    The response from the DSET API is a list of dictionaries with the
    following fields:

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
            "signal_external_id": blabla,
            "signal_device_external_description"; blabla,
            "signal_device_external_id": blabla,
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
    df_response["signal_external_id"] = signal["signal_external_id"]
    df_response["signal_device_external_description"] = signal["signal_device_external_description"]  # fmt: skip
    df_response["signal_device_external_id"] = signal["signal_device_external_id"]
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


def __get_lake_latest_signals(schema: str, db_engine: sa.engine.Engine) -> pd.DataFrame:
    df = pd.read_sql(
        f"""
            select distinct on (signal_id)
                signal_id,
                signal_value as max_last_ts_value,
                ts as max_last_ts
            from plants.{schema}.{TABLE_NAME__DSET_METERS_READINGS}
            order by signal_id, ts desc
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


def __query_data_latest(
    signal_id: str,
    api_key: str,
    query_params: dict,
    query_timeout: float,
    base_url: str,
) -> pd.DataFrame:
    """Query the latest data from the DSET API using the signal id"""

    response = httpx.get(
        url=base_url + f"/api/data/ISO_FORMAT/export/{signal_id}",
        params=query_params,
        headers={"Authorization": api_key},
        timeout=Timeout(timeout=query_timeout),
    )

    response.raise_for_status()

    df_response = pd.DataFrame(response.json())
    return df_response


def __append_new_signal_in_db(
    signal: T.Dict,
    api_key: str,
    base_url: str,
    engine: sa.engine.Engine,
    schema: str,
    query_timeout: float,
    dry_run: bool = True,
    apply_k_value: bool = True,
    sig_detail: bool = True,
):
    # we craft a request to fetch data from
    signal_id = signal["signal_id"]

    if signal["_merge"] == "both":
        date_from = signal["max_last_ts"]
    else:
        date_from = signal["signal_last_ts"]

    tz = pytz.timezone(signal["signal_tz"])
    date_to = datetime.datetime.now(tz=tz)

    logger.info(f"Updating signal {signal_id} from {date_from} to {date_to}")

    params = {
        "from": date_from.isoformat(),
        "to": date_to.isoformat(),
        "applykvalue": apply_k_value,
        "sig_detail": sig_detail,
    }

    logger.info(f"Querying data for signal with params: {params}")

    df_response = __query_data_latest(
        signal_id=signal_id,
        api_key=api_key,
        query_params=params,
        query_timeout=query_timeout,
        base_url=base_url,
    )

    df_new_signals = __extend_response(df_response, signal)

    logger.info(f"{len(df_new_signals)} new readings fetched")

    if not dry_run:
        df_new_signals.to_sql(
            con=engine,
            name=TABLE_NAME__DSET_METERS_READINGS,
            schema=schema,
            if_exists="append",
            index=False,
        )

    else:
        logger.info("Dry run, not committing to the database")


if __name__ == "__main__":
    app()
