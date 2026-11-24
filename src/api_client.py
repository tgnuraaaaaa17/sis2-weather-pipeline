"""Stage 1 - collect hourly weather data from the Open-Meteo Forecast API.

Open-Meteo needs no API key. One GET request is sent per city; the untouched
JSON responses are saved first (data/raw_api_responses.json) and then flattened
into one record per (city, hour) -> data/raw_data.csv.
"""
from __future__ import annotations

import json
import sys
import time

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src import config
from src.utils import get_logger, update_metrics, utc_now_iso, json_default

log = get_logger("api_client")


class ApiError(RuntimeError):
    """Raised when the API cannot be reached or returns an unusable response."""


def build_session() -> requests.Session:
    """HTTP session with automatic retries for transient errors (429, 5xx)."""
    session = requests.Session()
    retry = Retry(
        total=config.API_RETRIES, backoff_factor=1.0,
        status_forcelist=(429, 500, 502, 503, 504), allowed_methods=("GET",),
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers.update({"User-Agent": "sis2-weather-pipeline/1.0 (student project)"})
    return session


def build_params(city: dict) -> dict:
    return {
        "latitude": city["latitude"],
        "longitude": city["longitude"],
        "hourly": ",".join(config.HOURLY_VARIABLES),
        "forecast_days": config.FORECAST_DAYS,
        "past_days": config.PAST_DAYS,
        "timezone": config.TIMEZONE,
        "temperature_unit": "celsius",
        "wind_speed_unit": "kmh",
    }


def fetch_city(session: requests.Session, city: dict) -> dict:
    """Request one city. Raises ApiError for network errors / bad responses."""
    params = build_params(city)
    try:
        response = session.get(config.API_URL, params=params, timeout=config.API_TIMEOUT)
    except requests.exceptions.RequestException as exc:
        raise ApiError(f"network error for {city['name']}: {exc}") from exc

    if response.status_code != 200:
        raise ApiError(f"HTTP {response.status_code} for {city['name']}: {response.text[:200]}")
    try:
        payload = response.json()
    except ValueError as exc:
        raise ApiError(f"response for {city['name']} is not valid JSON") from exc
    if isinstance(payload, dict) and payload.get("error"):
        raise ApiError(f"API error for {city['name']}: {payload.get('reason')}")

    hourly = payload.get("hourly") if isinstance(payload, dict) else None
    if not hourly or "time" not in hourly:
        raise ApiError(f"response for {city['name']} has no 'hourly' block")
    expected = len(hourly["time"])
    for variable in config.HOURLY_VARIABLES:
        if variable not in hourly or len(hourly[variable]) != expected:
            raise ApiError(f"variable '{variable}' missing or incomplete for {city['name']}")

    return {"city": city["name"], "request_url": response.url, "params": params,
            "fetched_at": utc_now_iso(), "response": payload}


def flatten_response(city_name: str, payload: dict) -> list[dict]:
    """Turn the column-oriented hourly block into one dict per hour."""
    hourly = payload["hourly"]
    records = []
    for i, timestamp in enumerate(hourly["time"]):
        records.append({
            "record_id": f"{city_name}|{timestamp}",
            "city": city_name,
            "country": config.COUNTRY,
            "latitude": payload.get("latitude"),
            "longitude": payload.get("longitude"),
            "time": timestamp,
            "temperature_2m": hourly["temperature_2m"][i],
            "relative_humidity_2m": hourly["relative_humidity_2m"][i],
            "wind_speed_10m": hourly["wind_speed_10m"][i],
            "weather_code": hourly["weather_code"][i],
            "source": config.SOURCE_API,
        })
    return records


def collect() -> tuple[list[dict], list[dict], list[str]]:
    session = build_session()
    raw_responses, records, failed = [], [], []
    for city in config.CITIES:
        try:
            result = fetch_city(session, city)
        except ApiError as exc:
            log.error("%s", exc)
            failed.append(city["name"])
            continue
        city_records = flatten_response(city["name"], result["response"])
        raw_responses.append(result)
        records.extend(city_records)
        log.info("%-10s -> %d hourly records", city["name"], len(city_records))
        time.sleep(0.3)  # be polite to the free API
    return raw_responses, records, failed


def main() -> int:
    config.DATA_DIR.mkdir(exist_ok=True)
    log.info("Collecting data from %s", config.API_URL)
    raw_responses, records, failed = collect()

    if not records:
        log.error("No data collected. Check your internet connection.")
        return 1

    # 1) preserve the original responses BEFORE any cleaning
    config.RAW_JSON_PATH.write_text(
        json.dumps(raw_responses, ensure_ascii=False, indent=1, default=json_default), encoding="utf-8")
    # 2) flattened raw table (still uncleaned: nulls and odd values are kept)
    frame = pd.DataFrame(records, columns=config.RECORD_COLUMNS)
    frame.to_csv(config.RAW_CSV_PATH, index=False)

    update_metrics("api", {
        "endpoint": config.API_URL,
        "request_parameters": build_params(config.CITIES[0]) | {"latitude/longitude": "one request per city"},
        "cities_requested": len(config.CITIES),
        "cities_ok": len(raw_responses),
        "cities_failed": failed,
        "records_collected": len(frame),
        "time_min": frame["time"].min(),
        "time_max": frame["time"].max(),
        "missing_values_in_raw": {k: int(v) for k, v in frame.isna().sum().items() if v},
        "collected_at": utc_now_iso(),
    })
    log.info("Collected %d records (%d/%d cities OK) -> %s",
             len(frame), len(raw_responses), len(config.CITIES), config.RAW_CSV_PATH.name)

    if len(frame) < config.MIN_RECORDS:
        log.error("Only %d records collected, at least %d are required.", len(frame), config.MIN_RECORDS)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
