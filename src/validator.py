"""Stage 3 - data validation with Pandera.

Strategy for invalid data: QUARANTINE. Rows that break at least one rule are
removed from the main flow and saved (with their error messages) to
data/quarantine_invalid.csv; only valid rows continue to the cleaning stage.
"""
from __future__ import annotations

import sys

import pandas as pd
import pandera.pandas as pa
from pandera.pandas import Check, Column, DataFrameSchema

from src import config
from src.utils import get_logger, update_metrics
from datetime import timedelta

log = get_logger("validator")

TIME_FORMAT = "%Y-%m-%dT%H:%M"
TIME_REGEX = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}$"

# Human readable description of every rule (used in README / report).
RULES = [
    ("R1", "all columns", "Data types: numeric columns must be float, weather_code integer", "dtype"),
    ("R2", "record_id, city, time, temperature_2m, relative_humidity_2m, weather_code",
     "Required fields: no missing values (wind_speed_10m may be missing and is imputed later)", "not null"),
    ("R3", "temperature_2m", f"Range {config.TEMP_RANGE[0]}..{config.TEMP_RANGE[1]} degC", "range"),
    ("R4", "relative_humidity_2m", f"Range {config.HUMIDITY_RANGE[0]}..{config.HUMIDITY_RANGE[1]} %", "range"),
    ("R5", "wind_speed_10m", f"Range {config.WIND_RANGE[0]}..{config.WIND_RANGE[1]} km/h (no negative speed)", "range"),
    ("R6", "weather_code", "Allowed categorical values: WMO weather codes", "isin"),
    ("R7", "city", "Allowed categorical values: configured Kazakhstan cities", "isin"),
    ("R8", "time", "Date/time format YYYY-MM-DDTHH:MM and a real calendar date/time", "regex + parse"),
    ("R9", "time", f"Date window: {config.MIN_VALID_DATE} .. now + 30 days", "date range"),
    ("R10", "record_id", "Unique identifier: no duplicated record_id", "unique"),
    ("R11", "record_id", "Custom: record_id must equal '<city>|<time>'", "custom"),
    ("R12", "latitude, longitude, source", "Coordinates in valid ranges; source in {open-meteo, synthetic_test}", "range / isin"),
]


# ------------------------------------------------------------ check helpers
def _not_duplicated(series: pd.Series) -> pd.Series:
    return ~series.duplicated(keep="first")


def _parseable_time(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, format=TIME_FORMAT, errors="coerce").notna()


def _in_date_window(series: pd.Series) -> pd.Series:
    parsed = pd.to_datetime(series, format=TIME_FORMAT, errors="coerce")
    upper = pd.Timestamp.now() + timedelta(days=30)
    return parsed.between(pd.Timestamp(config.MIN_VALID_DATE), upper)   # NaT -> False


def _id_matches(frame: pd.DataFrame) -> pd.Series:
    return frame["record_id"] == frame["city"] + "|" + frame["time"]


def build_schema() -> DataFrameSchema:
    city_names = [c["name"] for c in config.CITIES]
    return DataFrameSchema(
        {
            "record_id": Column(str, nullable=False, checks=[
                Check(_not_duplicated, name="unique_record_id", error="record_id is duplicated")]),
            "city": Column(str, nullable=False, checks=[
                Check.isin(city_names, name="allowed_city", error="city not in allowed list")]),
            "country": Column(str, nullable=False, checks=[
                Check.isin([config.COUNTRY], name="allowed_country", error="unexpected country")]),
            "latitude": Column(float, nullable=False, coerce=True, checks=[
                Check.in_range(-90, 90, name="latitude_range", error="latitude out of range")]),
            "longitude": Column(float, nullable=False, coerce=True, checks=[
                Check.in_range(-180, 180, name="longitude_range", error="longitude out of range")]),
            "time": Column(str, nullable=False, checks=[
                Check.str_matches(TIME_REGEX, name="time_format", error="time must be YYYY-MM-DDTHH:MM"),
                Check(_parseable_time, name="time_parseable", error="time is not a real date/time"),
                Check(_in_date_window, name="time_in_window", error="time outside allowed date window")]),
            "temperature_2m": Column(float, nullable=False, coerce=True, checks=[
                Check.in_range(*config.TEMP_RANGE, name="temperature_range", error="temperature out of range")]),
            "relative_humidity_2m": Column(float, nullable=False, coerce=True, checks=[
                Check.in_range(*config.HUMIDITY_RANGE, name="humidity_range", error="humidity out of 0-100")]),
            "wind_speed_10m": Column(float, nullable=True, coerce=True, checks=[
                Check.in_range(*config.WIND_RANGE, name="wind_range", error="wind speed out of range")]),
            "weather_code": Column("Int64", nullable=False, coerce=True, checks=[
                Check.isin(sorted(config.WMO_CODES), name="allowed_weather_code", error="unknown weather code")]),
            "source": Column(str, nullable=False, checks=[
                Check.isin([config.SOURCE_API, config.SOURCE_TEST], name="allowed_source", error="unknown source")]),
        },
        checks=[Check(_id_matches, name="record_id_matches_city_time",
                      error="record_id != '<city>|<time>'")],
        strict=True,
        coerce=False,
    )


class SchemaStructureError(RuntimeError):
    """Columns missing / unexpected - the whole batch is unusable."""


def explain_failures(failure_cases: pd.DataFrame) -> tuple[dict[int, list[str]], pd.DataFrame]:
    """Convert Pandera failure cases to {row_index: [error, ...]} + a tidy table."""
    if failure_cases is None or failure_cases.empty:
        return {}, pd.DataFrame(columns=["row_index", "column", "check", "failure_case"])
    table = failure_cases.copy()
    table["row_index"] = pd.to_numeric(table["index"], errors="coerce")
    table["column"] = table["column"].fillna("DataFrame").astype(str)
    table["check"] = table["check"].astype(str)
    table["failure_case"] = table["failure_case"].astype(str).str.slice(0, 80)
    structural = table[table["row_index"].isna()]
    # schema-level problems (e.g. missing column) have no row index
    if not structural.empty and not structural["check"].str.contains("coerce", case=False).all():
        raise SchemaStructureError(structural[["column", "check"]].drop_duplicates().to_string(index=False))
    per_row: dict[int, list[str]] = {}
    for row in table.dropna(subset=["row_index"]).itertuples():
        per_row.setdefault(int(row.row_index), []).append(f"{row.column}: {row.check} [{row.failure_case}]")
    return per_row, table.dropna(subset=["row_index"])[["row_index", "column", "check", "failure_case"]]


def validate_dataframe(frame: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Return (valid_rows, invalid_rows_with_errors, error_table)."""
    frame = frame.reset_index(drop=True)
    schema = build_schema()
    try:
        schema.validate(frame, lazy=True)
        failure_cases = pd.DataFrame()
    except pa.errors.SchemaErrors as exc:
        failure_cases = exc.failure_cases
    per_row, error_table = explain_failures(failure_cases)

    invalid_mask = frame.index.isin(list(per_row))
    invalid = frame[invalid_mask].copy()
    invalid["validation_errors"] = [" | ".join(per_row[i]) for i in invalid.index]
    valid = frame[~invalid_mask].copy()
    return valid, invalid, error_table


def main() -> int:
    if not config.CONSUMED_PATH.exists():
        log.error("data/consumed_data.csv not found - run the consumer first")
        return 1
    frame = pd.read_csv(config.CONSUMED_PATH, dtype={"record_id": str, "city": str, "time": str,
                                                     "country": str, "source": str})
    log.info("Validating %d consumed records against the Pandera schema (%d rules)", len(frame), len(RULES))
    try:
        valid, invalid, error_table = validate_dataframe(frame)
    except SchemaStructureError as exc:
        log.error("Dataset structure is invalid:\n%s", exc)
        return 1

    valid.to_csv(config.VALID_PATH, index=False)
    invalid.to_csv(config.QUARANTINE_PATH, index=False)
    error_table.to_csv(config.VALIDATION_ERRORS_PATH, index=False)

    is_test = frame["source"].eq(config.SOURCE_TEST)
    invalid_is_test = invalid["source"].eq(config.SOURCE_TEST)
    test_total = int(is_test.sum())
    test_detected = int(invalid_is_test.sum())
    real_invalid = int((~invalid_is_test).sum())
    by_rule = (error_table.groupby(["column", "check"]).size().sort_values(ascending=False)
               if not error_table.empty else pd.Series(dtype=int))

    update_metrics("validation", {
        "received": len(frame), "valid": len(valid), "invalid": len(invalid),
        "real_records_invalid": real_invalid,
        "test_records_total": test_total, "test_records_detected": test_detected,
        "test_records_missed": test_total - test_detected,
        "rules_count": len(RULES),
        "errors_by_rule": {f"{col}: {chk}": int(n) for (col, chk), n in by_rule.items()},
        "strategy": "quarantine invalid rows in data/quarantine_invalid.csv; valid rows continue",
    })
    log.info("VALID=%d  INVALID=%d  (real invalid=%d, test records detected=%d/%d)",
             len(valid), len(invalid), real_invalid, test_detected, test_total)
    for row in invalid.head(10).itertuples():
        log.info("  invalid %-28s <- %s", row.record_id, row.validation_errors[:110])
    if test_total and test_detected < test_total:
        log.error("Validation schema MISSED %d test records", test_total - test_detected)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
