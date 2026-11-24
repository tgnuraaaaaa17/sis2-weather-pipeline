"""Stage 4 - cleaning and transformation with pandas + NumPy.

Input : data/validated_valid.csv   (rows that passed the Pandera schema)
Output: data/clean_data.csv plus summary tables (city summary, pivot, long format).
"""
from __future__ import annotations

import sys

import numpy as np
import pandas as pd

from src import config
from src.utils import get_logger, update_metrics

log = get_logger("cleaner")

PART_ORDER = ["Night", "Morning", "Afternoon", "Evening"]

FINAL_COLUMNS = [
    "record_id", "city", "country", "latitude", "longitude", "observation_time",
    "observation_date", "hour", "part_of_day", "temperature_c", "humidity_pct",
    "wind_speed_kmh", "wind_speed_ms", "weather_code", "weather_category",
    "weather_description", "dew_point_c", "wind_chill_c", "temp_zscore",
    "comfort_level", "wind_imputed",
]

WHY = {
    "missing values": "Wind speed can be missing in forecasts; dropping rows would waste valid temperature/humidity data, "
                      "so gaps are filled with the per-city median and flagged in 'wind_imputed'.",
    "duplicates": "Repeated API reads or retried Kafka deliveries would double-count hours and distort every average.",
    "test records": "Synthetic invalid records exist only to prove that validation works and must never reach the database.",
    "type conversion": "CSV stores everything as text; real datetime/int/category types are needed for time features, "
                       "memory efficiency and correct sorting.",
    "text normalisation": "Consistent city/country spelling prevents one city being split into several groups.",
    "numpy features": "Dew point, wind chill, m/s speed and z-scores turn raw measurements into comparable, "
                      "physically meaningful indicators (vectorised with NumPy).",
    "filtering": "Multi-condition filters isolate harsh-weather hours that matter for the analysis.",
    "groupby": "Per-city and per-weather-category aggregates answer the research questions.",
    "reshaping": "pivot_table gives a city x part-of-day matrix; melt creates a long format suitable for plotting/SQL.",
}


def part_of_day(hours: np.ndarray) -> np.ndarray:
    return np.select(
        [(hours >= 5) & (hours < 12), (hours >= 12) & (hours < 18), (hours >= 18) & (hours < 23)],
        ["Morning", "Afternoon", "Evening"], default="Night")


def add_numpy_features(df: pd.DataFrame) -> pd.DataFrame:
    temp = df["temperature_c"].to_numpy(dtype=float)
    rh = df["humidity_pct"].to_numpy(dtype=float)
    wind = df["wind_speed_kmh"].to_numpy(dtype=float)

    # Dew point (Magnus formula)
    a, b = 17.62, 243.12
    gamma = np.log(np.clip(rh, 1.0, 100.0) / 100.0) + a * temp / (b + temp)
    df["dew_point_c"] = np.round(b * gamma / (a - gamma), 2)

    # Wind chill (Environment Canada formula, valid for T <= 10 C and wind > 4.8 km/h)
    v016 = np.power(np.maximum(wind, 0.0), 0.16)
    chill = 13.12 + 0.6215 * temp - 11.37 * v016 + 0.3965 * temp * v016
    df["wind_chill_c"] = np.round(np.where((temp <= 10) & (wind > 4.8), chill, temp), 2)

    df["wind_speed_ms"] = np.round(wind / 3.6, 2)

    # per-city z-score of temperature (population std via NumPy)
    grouped = df.groupby("city", observed=True)["temperature_c"]
    mean = grouped.transform("mean").to_numpy()
    std = grouped.transform(lambda s: np.std(s.to_numpy())).to_numpy()
    df["temp_zscore"] = np.round(np.where(std == 0, 0.0, (temp - mean) / np.where(std == 0, 1, std)), 3)

    felt = df["wind_chill_c"].to_numpy()
    df["comfort_level"] = np.select(
        [felt < -20, felt < -10, felt < 0, felt < 10, felt < 20, felt < 30],
        ["Extreme cold", "Very cold", "Cold", "Cool", "Mild", "Warm"], default="Hot")
    df["part_of_day"] = part_of_day(df["hour"].to_numpy())
    return df


def run() -> int:
    if not config.VALID_PATH.exists():
        log.error("data/validated_valid.csv not found - run the validator first")
        return 1
    df = pd.read_csv(config.VALID_PATH, dtype={"record_id": str, "city": str, "time": str,
                                               "country": str, "source": str})
    stats: dict = {"input_records": len(df)}
    log.info("Cleaning %d validated records", len(df))

    # 1. exclude synthetic test records -------------------------------------------------
    is_test = df["source"].eq(config.SOURCE_TEST)
    stats["test_records_removed"] = int(is_test.sum())
    df = df[~is_test].copy()

    # 2. missing-value analysis + handling ---------------------------------------------
    missing = df.isna().sum()
    stats["missing_before"] = {k: int(v) for k, v in missing.items() if v}
    df["wind_imputed"] = df["wind_speed_10m"].isna().astype(int)
    city_median = df.groupby("city")["wind_speed_10m"].transform("median")
    df["wind_speed_10m"] = df["wind_speed_10m"].fillna(city_median).fillna(df["wind_speed_10m"].median())
    stats["wind_values_imputed"] = int(df["wind_imputed"].sum())
    stats["missing_after"] = int(df[["temperature_2m", "relative_humidity_2m", "wind_speed_10m",
                                     "weather_code"]].isna().sum().sum())

    # 3. duplicates ---------------------------------------------------------------------
    dup_mask = df.duplicated(subset="record_id", keep="first")
    stats["duplicates_removed"] = int(dup_mask.sum())
    df = df[~dup_mask].copy()

    # 4. data types ---------------------------------------------------------------------
    df["time"] = pd.to_datetime(df["time"], format="%Y-%m-%dT%H:%M")
    df["weather_code"] = df["weather_code"].astype("int64")
    for col in ("temperature_2m", "relative_humidity_2m", "wind_speed_10m", "latitude", "longitude"):
        df[col] = df[col].astype("float64")

    # 5. text normalisation --------------------------------------------------------------
    df["city"] = df["city"].str.strip().str.title()
    df["country"] = df["country"].str.strip().str.upper()
    df["weather_category"] = df["weather_code"].map(lambda c: config.WMO_CODES[c][0])
    df["weather_description"] = df["weather_code"].map(lambda c: config.WMO_CODES[c][1])
    df["city"] = df["city"].astype("category")

    # 6. rename + time features -----------------------------------------------------------
    df = df.rename(columns={"temperature_2m": "temperature_c", "relative_humidity_2m": "humidity_pct",
                            "wind_speed_10m": "wind_speed_kmh"})
    df["hour"] = df["time"].dt.hour
    df["observation_date"] = df["time"].dt.strftime("%Y-%m-%d")
    df["observation_time"] = df["time"].dt.strftime("%Y-%m-%dT%H:%M")

    # 7. NumPy operations -------------------------------------------------------------------
    df = add_numpy_features(df)
    df = df.sort_values(["city", "observation_time"]).reset_index(drop=True)

    # 8. multi-condition filtering ----------------------------------------------------------
    windy_cold = df[(df["temperature_c"] <= 0) & (df["wind_speed_kmh"] >= 20)]
    humid_day = df[(df["humidity_pct"] >= 80) & (df["hour"].between(8, 20))
                   & (df["weather_category"].isin(["Rain", "Showers", "Drizzle", "Snow", "Fog"]))]
    stats["filter_windy_cold"] = len(windy_cold)
    stats["filter_humid_daytime_precipitation_or_fog"] = len(humid_day)
    windy_cold.to_csv(config.DATA_DIR / "filtered_windy_cold.csv", index=False)

    # 9. GroupBy + aggregation ---------------------------------------------------------------
    city_summary = df.groupby("city", observed=True).agg(
        records=("record_id", "count"),
        temp_mean=("temperature_c", "mean"), temp_min=("temperature_c", "min"),
        temp_max=("temperature_c", "max"), temp_std=("temperature_c", "std"),
        humidity_mean=("humidity_pct", "mean"),
        wind_mean=("wind_speed_kmh", "mean"), wind_max=("wind_speed_kmh", "max"),
    ).round(2).reset_index()
    category_summary = df.groupby("weather_category").agg(
        hours=("record_id", "count"), temp_mean=("temperature_c", "mean"),
        humidity_mean=("humidity_pct", "mean"), wind_mean=("wind_speed_kmh", "mean"),
    ).round(2).reset_index().sort_values("hours", ascending=False)
    city_summary.to_csv(config.DATA_DIR / "summary_by_city.csv", index=False)
    category_summary.to_csv(config.DATA_DIR / "summary_by_weather_category.csv", index=False)

    # 10. reshaping: pivot_table + melt --------------------------------------------------------
    pivot = pd.pivot_table(df, index="city", columns="part_of_day", values="temperature_c",
                           aggfunc="mean", observed=True).reindex(columns=PART_ORDER).round(2)
    pivot.to_csv(config.DATA_DIR / "pivot_city_part_of_day.csv")
    long_format = city_summary.melt(id_vars="city", var_name="metric", value_name="value")
    long_format.to_csv(config.DATA_DIR / "long_city_metrics.csv", index=False)

    final = df[FINAL_COLUMNS].copy()
    final["city"] = final["city"].astype(str)
    final.to_csv(config.CLEAN_PATH, index=False)

    stats.update({
        "records_after_cleaning": len(final),
        "pivot_shape": list(pivot.shape), "long_format_shape": list(long_format.shape),
        "city_summary_rows": len(city_summary),
        "why_each_step": WHY,
    })
    update_metrics("cleaning", stats)
    log.info("missing before=%s | imputed wind=%d | duplicates removed=%d | test removed=%d",
             stats["missing_before"] or "none", stats["wind_values_imputed"],
             stats["duplicates_removed"], stats["test_records_removed"])
    log.info("filter windy&cold=%d | humid daytime wet/fog=%d", len(windy_cold), len(humid_day))
    log.info("CLEAN records: %d -> %s", len(final), config.CLEAN_PATH.name)
    return 0


if __name__ == "__main__":
    sys.exit(run())
