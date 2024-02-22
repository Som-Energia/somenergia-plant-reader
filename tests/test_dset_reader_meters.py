import pandas as pd
import pytest


def mock__get_lake_latest_signals(
    signal_id: int = 1,
    max_last_ts_value: float = 1.0,
    max_last_ts: pd.Timestamp = pd.Timestamp("2022-12-02 00:00:00"),
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
    signal_last_ts: pd.Timestamp = "2024-02-13 23:45:00",
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
        }
    ]

    return pd.DataFrame(mock_response)


def test_table_creation_fails_if_exists(mocker):
    import scripts.read_dset_meters

    mocker.patch(
        "scripts.read_dset_meters._get_lake_latest_signals",
        mock__get_lake_latest_signals,
    )

    result = scripts.read_dset_meters._get_lake_latest_signals()
    expected = mock__get_lake_latest_signals()

    pd.testing.assert_frame_equal(result, expected)


@pytest.mark.skip(reason="Not implemented")
def test_no_outdated_data_in_lake_does_nothing():
    pass


@pytest.mark.skip(reason="Not implemented")
def test_only_outdated_data_updates_lake():
    pass


@pytest.mark.skip(reason="Not implemented")
def test_new_meters_updates_lake():
    pass
