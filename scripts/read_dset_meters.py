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


def validate_date_is_utc(date: datetime.datetime) -> datetime.datetime:
    if date is not None and date.strftime("%Z") != "UTC":
        raise typer.BadParameter("date must be in UTC")
    return date


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
    return_null_values: bool = typer.Option(
        False, "--return-null-values", help="Put null on missing values", is_flag=True
    ),
    look_back_days: int = typer.Option(
        7,
        "--look-back-days",
        help="Number of days to look back from last timestamp when table is empty",
    ),
    date_from: T.Optional[datetime.datetime] = typer.Option(
        None,
        "--date-from",
        help="Start date to fetch data. Overrides --look-back-days.",
        formats=[
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%d %H:%M:%S%z",
            "%Y-%m-%d%z",
        ],
        callback=validate_date_is_utc,
    ),
    date_to: T.Optional[datetime.datetime] = typer.Option(
        None,
        "--date-to",
        help="End date to fetch data. Overrides --look-back-days.",
        formats=[
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%d %H:%M:%S%z",
            "%Y-%m-%d%z",
        ],
        callback=validate_date_is_utc,
    ),
):
    """Get historic readings from the DSET API and compare with what we have"""

    if any([date_from, date_to]) and not all([date_from, date_to]):
        logger.error(
            "If date_from or date_to are specified, both need to be specified."
        )
        raise typer.Exit(code=1)

    should_look_back = not all([date_from, date_to])

    queryparams = {
        "sig_detail": sig_detail,
        "applykvalue": apply_k_value,
        "returnNullValues": return_null_values,
    }

    logger.info("Fetching groups from the DSET API")

    # we fetch the groups
    queried_at_ts = datetime.datetime.now(tz=pytz.utc)

    response = httpx.get(
        base_url + "/api/groups",
        params=queryparams,
        headers={"Authorization": apikey},
        timeout=Timeout(timeout=query_timeout),
    )

    response.raise_for_status()

    logger.info("Querying groups from the DSET API at %s", response.url)

    filter_frequency = "15 minutes"
    filtered_signals = __transform_group_response(
        response.json(),
        filter_frequency,
    )

    logger.info(
        "%s signals left after filtering by frequency %s",
        len(filtered_signals),
        filter_frequency,
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
        "signal_last_ts": "datetime64[ns]",
        "signal_last_value": "float64",
        "signal_unit": "string",
        "signal_external_id": "string",
        "signal_device_external_description": "string",
        "signal_device_external_id": "string",
        "queried_at": "datetime64[ns, UTC]",
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
        "signal_last_ts": sa.DateTime(timezone=False),
        "signal_last_value": sa.Float,
        "signal_unit": sa.String,
        "signal_external_id": sa.String,
        "signal_device_external_description": sa.String,
        "signal_device_external_id": sa.String,
        "queried_at": sa.DateTime(timezone=True),
    }

    df_last_signals = pd.DataFrame(filtered_signals)
    df_last_signals["queried_at"] = queried_at_ts + response.elapsed

    found_timezones = df_last_signals["signal_tz"].unique()

    if len(found_timezones) != 1 or found_timezones[0] != "UTC":
        # this is because API endpoint uses /ISOFORMAT/ and we assume UTC
        logger.error(
            "Meter signals are mixing timezones=%s and we assume UTC. Aborting.",
            found_timezones,
        )
        raise typer.Exit(code=1)

    df_last_signals = df_last_signals.astype(_signals_dtype)

    engine = sa.create_engine(dbapi)

    # assess if table exists
    insp = sa.inspect(engine)
    table_exists = insp.has_table(
        TABLE_NAME__DSET_METERS_READINGS,
        schema=schema,
    )

    if not table_exists:
        # we update fields to match the lake table
        df_to_lake_new = __extend_df_first_insert(df_last_signals, queried_at_ts)

        logger.info(
            "Table %s does not exist, creating it as we insert the data",
            f"{schema}.{TABLE_NAME__DSET_METERS_READINGS}",
        )

        if dry_run:
            logger.info(
                "Dry run, not committing %s rows to the database",
                len(df_to_lake_new),
            )
            return

        df_to_lake_new.to_sql(
            con=engine,
            name=TABLE_NAME__DSET_METERS_READINGS,
            schema=schema,
            if_exists="fail",
            index=False,
            dtype=_signals_sql_types,
        )

        logger.info("%s signals inserted.", len(df_last_signals))
    else:
        logger.info("Table %s exists", f"{schema}.{TABLE_NAME__DSET_METERS_READINGS}")

        if should_look_back:
            logger.info(
                "Comparing last readings (%s days ago) with stored ones",
                look_back_days,
            )

            df_lake_meters = __get_lake_latest_signals(schema, engine)

            df_in_lake_outdated = __resolve_outdated_signals(
                df_api=df_last_signals,
                df_lake=df_lake_meters,
            )

            logger.info(
                "Found %s signals that need updating",
                len(df_in_lake_outdated),
            )
        else:
            # we fetch all the signals
            logger.info(
                "Both --date-from and --date-to passed. Reading signals from %s to %s",
                date_from,
                date_to,
            )
            df_in_lake_outdated = df_last_signals

        # TODO: parallelize this, transactions are independent
        for _, signal in df_in_lake_outdated.iterrows():
            __append_new_signal_in_db(
                signal,
                api_key=apikey,
                base_url=base_url,
                apply_k_value=apply_k_value,
                return_null_values=return_null_values,
                sig_detail=sig_detail,
                engine=engine,
                schema=schema,
                query_timeout=query_timeout,
                dry_run=dry_run,
                look_back_days=look_back_days,
                date_from=date_from,
                date_to=date_to,
            )


def __extend_df_first_insert(
    df_last_signals: pd.DataFrame,
    queried_at_ts: datetime.datetime,
) -> pd.DataFrame:
    """Extends a dataframe to match the lake table in the first insert

    Parameters
    ----------
    df_last_signals : pd.DataFrame
        A dataframe with the last signals fetched from the DSET API
    queried_at_ts : datetime.datetime
        An UTC aware timestamp of when the data was queried

    Returns
    -------
    pd.DataFrame
        A dataframe with the last signals fetched from the DSET API,
        plus the queried_at timestamp and the ts and signal_value fields set to
        the last ts and value
    """

    # we set the current ts and value as the last ts and value
    df_last_signals["ts"] = df_last_signals["signal_last_ts"]
    df_last_signals["signal_value"] = df_last_signals["signal_last_value"]

    # we set the queried_at timestamp
    df_last_signals["queried_at"] = queried_at_ts

    return df_last_signals


def __resolve_outdated_signals(
    df_api: pd.DataFrame,
    df_lake: pd.DataFrame,
) -> pd.DataFrame:
    """Filter outdated signals comparing data from api with data in lake

    Performs a full outer join to get the last readings and the last stored readings.

    It uses two columns that need to be present in the dataframes:
    `signal_last_ts` is the last ts received from the API, and `max_last_ts` is
    the last ts in the lake

    If it's a new signal from the api, if will show up only on the left side of the join.
    In this case, we set the last stored ts and value as the current ts and value

    Parameters
    ----------
    df_api : pd.DataFrame
        dataframe with incoming data from the DSET API
    df_lake : pd.DataFrame
        dataframe with the last stored readings in the lake

    Returns
    -------
    pd.DataFrame
        A dataframe with outdated. We say that the outdated signals are the
        ones that are present in the lake, but have a newer reading, and new
        signals not present in the lake.
    """

    df_recent = df_api.merge(
        df_lake,
        left_on="signal_id",
        right_on="signal_id",
        how="outer",
        indicator=True,
        validate="one_to_one",  # ensures that signal_id is unique in both dataframes
        suffixes=("__dset", "__som"),
    )

    _df_outdated_in_lake = df_recent.query(
        "_merge == 'both' and signal_last_ts > max_last_ts"
    )

    _df_new_from_api = df_recent.query("_merge == 'left_only'")
    _df_new_from_api["max_last_ts"] = _df_new_from_api["signal_last_ts"]
    _df_new_from_api["max_last_ts_value"] = _df_new_from_api["signal_last_value"]

    df_in_lake_outdated = pd.concat([_df_outdated_in_lake, _df_new_from_api], axis=0)
    df_in_lake_outdated.replace({np.nan: None}, inplace=True)

    return df_in_lake_outdated


def __extend_response(
    df_response: pd.DataFrame,
    signal: dict,
) -> pd.DataFrame:
    """Extend the response from /api/data/{signal_id} to match the schema in the lake table

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
            "signal_tz": "UTC",
            "signal_last_ts": "2024-02-13 23:45:00",
            "signal_last_value": 0,
            "signal_unit": "kWh"
            "signal_external_id": some_uuid,
            "signal_device_external_description"; some_external_description,
            "signal_device_external_id": some_external_uuid,
            "ts": "2021-01-01 00:00:00", # new from the response
            "signal_value": 77 # new from the response
        }
    ]
    """

    # ts, value
    df_response.rename(columns={"value": "signal_value"}, inplace=True)

    # rest coming from /groups
    df_response["group_id"] = signal["group_id"]
    df_response["group_code"] = signal["group_code"]
    df_response["group_name"] = signal["group_name"]
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
    df_response["queried_at"] = signal["queried_at"]
    df_response["signal_last_ts"] = signal["signal_last_ts"]
    df_response["signal_last_value"] = signal["signal_last_value"]

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
                    "signal_id": 11,
                    "signal_code": "some_code",
                    "signal_description": "some_description",
                    "signal_frequency": "15 minutes",
                    "signal_type": "absolute",
                    "signal_is_virtual": False,
                    "signal_tz": "UTC",
                    "signal_last_ts": "2024-02-13 23:45:00",
                    "signal_last_value": 0,
                    "signal_unit": "kWh",
                    "signal_external_id": "some_external_id_1",
                    "signal_device_external_description": "some_device_external_description_1",
                    "signal_device_external_id": "some_device_external_id_1",
                },
                {
                    "signal_id": 12,
                    "signal_code": "some_other_code",
                    "signal_description": "some_other_description",
                    "signal_frequency": "2222 minutes", # not the same frequency
                    "signal_type": "absolute",
                    "signal_is_virtual": False,
                    "signal_tz": "UTC",
                    "signal_last_ts": "2024-02-13 21:45:00",
                    "signal_last_value": 10,
                    "signal_unit": "kWh",
                    "signal_external_id": "some_external_id_2",
                    "signal_device_external_description": "some_device_external_description_2",
                    "signal_device_external_id": "some_device_external_id_2",
                },
            ],
        },
    ]

    >>> __transform_group_response(group_response_json, "15 minutes")
    [
        {
            "group_id": 1,
            "group_code": "a",
            "group_name": "b",
            "signal_id": 11,
            "signal_code": "some_code",
            "signal_description": "some_description",
            "signal_frequency": "15 minutes",
            "signal_type": "absolute",
            "signal_is_virtual": False,
            "signal_tz": "UTC",
            "signal_last_ts": "2024-02-13 23:45:00",
            "signal_last_value": 0,
            "signal_unit": "kWh",
            "signal_external_id": "some_external_id_1",
            "signal_device_external_description": "some_device_external_description_1",
            "signal_device_external_id": "some_device_external_id_1",
        }
    ]
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


def __get_lake_latest_signals(
    schema: str,
    engine: sa.engine.Engine,
) -> pd.DataFrame:
    """Get the latest signals from the lake table using a raw SQL query

    We ensure times are naïve in UTC to avoid timezone issues.
    """

    df = pd.read_sql(
        f"""select distinct on (signal_id)
                signal_id,
                signal_value as max_last_ts_value,
                (ts at time zone signal_tz) at time zone 'UTC' as max_last_ts -- ensure ts is naïve but in UTC
            from plants.{schema}.{TABLE_NAME__DSET_METERS_READINGS}
            order by signal_id, ts desc
            """,
        engine,
    ).astype(
        {
            "signal_id": "int64",
            "max_last_ts_value": "float64",
            "max_last_ts": "datetime64[ns]",
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

    # we filter out None values
    response_json = [t for t in response.json() if t is not None]
    df_response = pd.DataFrame(response_json)

    return df_response


def __append_new_signal_in_db(
    signal: T.Dict,
    api_key: str,
    base_url: str,
    engine: sa.engine.Engine,
    schema: str,
    query_timeout: float,
    look_back_days: int,
    dry_run: bool = True,
    apply_k_value: bool = True,
    sig_detail: bool = True,
    return_null_values: bool = True,
    date_from: T.Optional[datetime.datetime] = None,
    date_to: T.Optional[datetime.datetime] = None,
):
    """Downloads data from the DSET API and appends it to the lake table given some time references

    The signal is a dict that has a signal_id, signal_last_ts, signal_tz, and other fields. This will query
    the DSET endpoint using the signal_id, and if this signal_id is not present in the lake, it will fetch
    as much data between now and the last `look_back_days` days.

    Otherwise, it will fetch data from signal_last_ts to now to update the lake table.

    if date_from and date_to are specified, this will override this behaviour and it will attempt to download data
    in this range only.

    Parameters
    ----------
    signal : T.Dict
        A signal dictionary obtainer from a dataframe
    api_key : str
        the api key to the DSET API
    base_url : str
        the base url of the DSET API
    engine : sa.engine.Engine
        A working SQLAlchemy engine
    schema : str
        The schema where the lake table is located
    query_timeout : float
        Timeout for the query, in seconds
    look_back_days : int
        How many days to look back if the signal is not present in the lake
    dry_run : bool, optional
        Don't attempt to insert anything, by default True
    apply_k_value : bool, optional
        Pass the applyKValue to the query. This tells the API to apply a conversion factor.
        Otherwise values will not be properly scaled, by default True
    sig_detail : bool, optional
        Pass sigDetail to the DSET API so that it will return UUIDs, by default True
    return_null_values : bool, optional
        Pass returnNullValues to the DSET API so that it will return null values, by default True
    date_from : Optional[datetime.datetime], optional
        An optional start date (UTC) to override the behaviour of look_back_days, by default None
    date_to : Optional[datetime.datetime], optional
        An optional final date (UTC) to override the behaviour of look_back_days, by default None
    """

    if any([date_from, date_to]) and not all([date_from, date_to]):
        logger.error(
            "If date_from or date_to are specified, both need to be specified."
        )
        raise typer.Exit(code=1)

    if all([date_from, date_to]):
        if date_from > date_to:
            logger.error("date_from must be less than date_to.")
            raise typer.Exit(code=1)
    else:
        # We assume max_last_ts has the same timezone as signal_last_ts
        # ensure we use UTC regardless of the timezone, silently
        date_from = (
            pd.to_datetime(signal["max_last_ts"])
            .tz_localize(signal["signal_tz"])
            .tz_convert("UTC")
        )

        date_to = (
            pd.to_datetime(signal["signal_last_ts"])
            .tz_localize(signal["signal_tz"])
            .tz_convert("UTC")
        )

        if signal["_merge"] == "both":
            # it's a signal that is present in the lake, but outdated
            logger.info(
                "Signal %s (%s, %s) is present in the lake, but outdated since %s.",
                signal["signal_id"],
                signal["group_name"],
                signal["signal_description"],
                date_from,
            )
        else:
            # it's a new signal incoming from the api, we fetch look_back_days of data
            date_from = date_from - datetime.timedelta(days=look_back_days)
            logger.info(
                "New signal %s detected, not present in the lake. "
                "Fetching %s days of data.",
                signal["signal_id"],
                look_back_days,
            )

    params = {
        "from": date_from.isoformat().replace("+00:00", "Z"),
        "to": date_to.isoformat().replace("+00:00", "Z"),
        "applykvalue": apply_k_value,
        "sig_detail": sig_detail,
        "returnNullValues": return_null_values,
    }

    logger.debug(
        "Querying data for signal %s with params: %s",
        signal["signal_id"],
        params,
    )

    df_response = __query_data_latest(
        signal_id=signal["signal_id"],
        api_key=api_key,
        query_params=params,
        query_timeout=query_timeout,
        base_url=base_url,
    )

    if "ts" not in df_response.columns or "value" not in df_response.columns:
        logger.error(
            "Response from the DSET API for signal %s is missing the ts or value fields",
            signal["signal_id"],
        )
        return

    if len(df_response) == 0:
        logger.info(
            "No new readings for signal %s",
            signal["signal_id"],
        )
        return

    df_new_signals = __extend_response(df_response, signal)

    # add queried_at timestamp
    # df_new_signals["queried_at"] = date_to

    logger.info("%s new readings fetched", len(df_new_signals))

    if dry_run:
        logger.warning("Dry run, not committing to the database")
        return

    logger.info("Committing results to the database")

    df_new_signals.to_sql(
        con=engine,
        name=TABLE_NAME__DSET_METERS_READINGS,
        schema=schema,
        if_exists="append",
        index=False,
    )


if __name__ == "__main__":
    app()
