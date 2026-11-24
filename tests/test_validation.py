"""Validation testing: the Pandera schema must catch deliberately invalid records.

Run with:  python -m pytest tests -q      (or)      python -m tests.test_validation
"""
import pandas as pd

from src import config
from src.validator import validate_dataframe


def _good_row(city="Almaty", hour="12", code=3):
    time = f"2026-11-24T{hour}:00"
    return {"record_id": f"{city}|{time}", "city": city, "country": "KZ", "latitude": 43.25,
            "longitude": 76.95, "time": time, "temperature_2m": 1.5, "relative_humidity_2m": 70.0,
            "wind_speed_10m": 12.0, "weather_code": code, "source": config.SOURCE_API}


def _frame(rows):
    return pd.DataFrame(rows, columns=config.RECORD_COLUMNS)


def test_valid_rows_pass():
    valid, invalid, _ = validate_dataframe(_frame([_good_row(hour="10"), _good_row(hour="11")]))
    assert len(valid) == 2 and len(invalid) == 0


def test_invalid_records_are_detected():
    bad = [
        dict(_good_row(hour="13"), relative_humidity_2m=135.0),
        dict(_good_row(hour="14"), temperature_2m=None),
        dict(_good_row(hour="15"), weather_code=999),
        dict(_good_row(hour="16"), wind_speed_10m=-5.0),
        dict(_good_row(hour="17"), city="Atlantis", record_id="Atlantis|2026-11-24T17:00"),
        dict(_good_row(hour="18"), time="2026-13-45T25:61", record_id="Almaty|2026-13-45T25:61"),
    ]
    rows = [_good_row(hour="10")] + bad
    valid, invalid, errors = validate_dataframe(_frame(rows))
    assert len(valid) == 1
    assert len(invalid) == len(bad)
    assert invalid["validation_errors"].str.len().gt(0).all()


def test_duplicate_record_id_is_detected():
    valid, invalid, _ = validate_dataframe(_frame([_good_row(), _good_row()]))
    assert len(valid) == 1 and len(invalid) == 1


if __name__ == "__main__":
    test_valid_rows_pass()
    test_invalid_records_are_detected()
    test_duplicate_record_id_is_detected()
    print("All validation tests passed.")
