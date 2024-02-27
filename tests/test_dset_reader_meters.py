import httpx
import pandas as pd
import pytest


def mock__get_lake_latest_signals(
    max_last_ts: pd.Timestamp,
    signal_id: int = 1,
    max_last_ts_value: float = 1.0,
):
    mock_response = [
        {
            "signal_id": signal_id,
            "max_last_ts_value": max_last_ts_value,
            "max_last_ts": max_last_ts,
        }
    ]

    return pd.DataFrame(mock_response)


def mock__query_data_latest(
    signal_last_ts: pd.Timestamp,
):
    mock_response = [
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
            "signal_last_ts": signal_last_ts,
            "signal_last_value": 0,
            "signal_unit": "kWh",
            "signal_external_id": "some_external_id",
            "signal_device_external_description": "some_device_external_description",
            "signal_device_external_id": "some_device_external_id",
        }
    ]

    return pd.DataFrame(mock_response)


def test_get(mocker, cli_runner):
    import json

    import scripts.read_dset_meters

    # craft response
    mock_response_content = [
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
            "signal_last_ts": "2022-12-02T00:00:00",
            "signal_last_value": 0,
            "signal_unit": "kWh",
            "signal_external_id": "some_external_id",
            "signal_device_external_description": "some_device_external_description",
            "signal_device_external_id": "some_device_external_id",
        }
    ]

    mock_response = httpx.Response(
        200,
        content=json.dumps(mock_response_content).encode(),
        request=httpx.Request("GET", "test"),
    )

    mocker.patch("scripts.read_dset_meters.httpx.get", return_value=mock_response)

    params = [
        "--api-key",
        "fake-api-key",
        "--schema",
        "lake-test",
        "--db-url",
        "postgresql://jardiner:password@mock-dbdades.somenergia.lan:5432/plants",
    ]

    result = cli_runner.invoke(scripts.read_dset_meters.app, params)

    assert result.exit_code == 0


def test_mocker_example_compare_frames(mocker):
    import scripts.read_dset_meters

    DEFAULT_MAX_LAST_TS = pd.Timestamp("2022-12-02 00:00:00")

    mocker.patch(
        "scripts.read_dset_meters.__get_lake_latest_signals",
        mock__get_lake_latest_signals,
    )

    result = scripts.read_dset_meters.__get_lake_latest_signals(
        max_last_ts=DEFAULT_MAX_LAST_TS,
    )
    expected = mock__get_lake_latest_signals(max_last_ts=DEFAULT_MAX_LAST_TS)

    pd.testing.assert_frame_equal(result, expected)


@pytest.mark.skip(reason="Not implemented")
def test_no_outdated_data_in_lake_does_nothing():
    """Tests that the script does nothing if the data in the lake is up to date."""

    # 1. Given data from the api with a given date

    # 2. And a lake holding data with the same timestamp
    # 3. When the script is run
    # 4. Then the lake is not updated

    pass


@pytest.mark.skip(reason="Not implemented")
def test_only_outdated_data_updates_lake():
    """Tests that the script updates lake data only for outdated data.

    1. Given data from the api with a given date
    2. And a lake holding data with the same timestamp, and some with older timestamps
    3. When the script is run
    4. Then the lake is not updated for rows up to date
    5. And the lake is updated for rows with older timestamps
    """
    pass


@pytest.mark.skip(reason="Not implemented")
def test_new_meters_updates_lake():
    """Tests that script inserts new meters into the lake if they are not already present.


    1. Given data from the api with a given date, with a signal_code not present in the lake
    2. And a lake holding data with the same timestamp, but not the extra signal_code from the api
    3. When the script is run
    4. Then the lake is not updated for existing rows, and a new row is inserted for the new signal_code
    """
    pass
