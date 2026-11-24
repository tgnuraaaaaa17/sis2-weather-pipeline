"""Stage 5 - store cleaned records in SQLite3 (idempotent loading).

Idempotency: the primary key is record_id and every insert is an UPSERT
(INSERT ... ON CONFLICT(record_id) DO UPDATE), so loading the same dataset twice
neither duplicates rows nor changes stored values.
"""
from __future__ import annotations

import argparse
import hashlib
import re
import sqlite3
import sys

import pandas as pd

from src import config
from src.utils import get_logger, update_metrics

log = get_logger("loader")

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS cities (
    city        TEXT PRIMARY KEY,
    country     TEXT NOT NULL,
    latitude    REAL NOT NULL CHECK (latitude  BETWEEN -90  AND 90),
    longitude   REAL NOT NULL CHECK (longitude BETWEEN -180 AND 180)
);

CREATE TABLE IF NOT EXISTS weather_observations (
    record_id           TEXT    PRIMARY KEY,
    city                TEXT    NOT NULL REFERENCES cities(city),
    observation_time    TEXT    NOT NULL,
    observation_date    TEXT    NOT NULL,
    hour                INTEGER NOT NULL CHECK (hour BETWEEN 0 AND 23),
    part_of_day         TEXT    NOT NULL,
    temperature_c       REAL    NOT NULL CHECK (temperature_c BETWEEN -90 AND 60),
    humidity_pct        REAL    NOT NULL CHECK (humidity_pct BETWEEN 0 AND 100),
    wind_speed_kmh      REAL    NOT NULL CHECK (wind_speed_kmh >= 0),
    wind_speed_ms       REAL    NOT NULL,
    weather_code        INTEGER NOT NULL,
    weather_category    TEXT    NOT NULL,
    weather_description TEXT    NOT NULL,
    dew_point_c         REAL,
    wind_chill_c        REAL,
    temp_zscore         REAL,
    comfort_level       TEXT,
    wind_imputed        INTEGER NOT NULL DEFAULT 0 CHECK (wind_imputed IN (0, 1)),
    UNIQUE (city, observation_time)
);

CREATE INDEX IF NOT EXISTS idx_obs_city_time ON weather_observations (city, observation_time);
"""

OBS_COLUMNS = ["record_id", "city", "observation_time", "observation_date", "hour", "part_of_day",
               "temperature_c", "humidity_pct", "wind_speed_kmh", "wind_speed_ms", "weather_code",
               "weather_category", "weather_description", "dew_point_c", "wind_chill_c",
               "temp_zscore", "comfort_level", "wind_imputed"]

UPSERT_OBS_SQL = (
    f"INSERT INTO weather_observations ({', '.join(OBS_COLUMNS)}) "
    f"VALUES ({', '.join('?' for _ in OBS_COLUMNS)}) "
    "ON CONFLICT(record_id) DO UPDATE SET "
    + ", ".join(f"{c}=excluded.{c}" for c in OBS_COLUMNS if c != "record_id")
)
UPSERT_CITY_SQL = (
    "INSERT INTO cities (city, country, latitude, longitude) VALUES (?, ?, ?, ?) "
    "ON CONFLICT(city) DO UPDATE SET country=excluded.country, "
    "latitude=excluded.latitude, longitude=excluded.longitude"
)


def connect(path=config.DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def count_rows(conn: sqlite3.Connection) -> int:
    return conn.execute("SELECT COUNT(*) FROM weather_observations").fetchone()[0]


def table_checksum(conn: sqlite3.Connection) -> str:
    """SHA-256 over the whole table content - detects ANY change of stored data."""
    digest = hashlib.sha256()
    for row in conn.execute("SELECT * FROM weather_observations ORDER BY record_id"):
        digest.update(repr(row).encode("utf-8"))
    return digest.hexdigest()


def load_dataframe(conn: sqlite3.Connection, df: pd.DataFrame) -> dict:
    """One loading run. Returns before/after counts."""
    before = count_rows(conn)
    cities = df[["city", "country", "latitude", "longitude"]].drop_duplicates("city")
    obs = df[OBS_COLUMNS].astype(object).where(df[OBS_COLUMNS].notna(), None)
    try:
        with conn:   # one transaction: all rows or nothing
            conn.executemany(UPSERT_CITY_SQL, cities.itertuples(index=False, name=None))
            conn.executemany(UPSERT_OBS_SQL, obs.itertuples(index=False, name=None))
    except sqlite3.Error as exc:
        log.error("Database error, transaction rolled back: %s", exc)
        raise
    after = count_rows(conn)
    return {"rows_before": before, "rows_attempted": len(df), "rows_after": after,
            "rows_inserted": after - before}


def parse_sql_file(path=config.VERIFY_SQL_PATH) -> dict[str, str]:
    queries: dict[str, str] = {}
    for block in re.split(r"^-- name:\s*", path.read_text(encoding="utf-8"), flags=re.M)[1:]:
        name, _, sql = block.partition("\n")
        queries[name.strip()] = sql.strip().rstrip(";")
    return queries


def run_verification(conn: sqlite3.Connection) -> dict:
    results = {}
    for name, sql in parse_sql_file().items():
        cursor = conn.execute(sql)
        columns = [d[0] for d in cursor.description]
        rows = cursor.fetchall()
        results[name] = {"columns": columns, "rows": [list(r) for r in rows[:12]], "row_count": len(rows)}
        log.info("SQL %-18s -> %s", name, rows[:3] if rows else "(no rows)")
    results["pragma_integrity_check"] = conn.execute("PRAGMA integrity_check").fetchone()[0]
    results["pragma_foreign_key_violations"] = len(conn.execute("PRAGMA foreign_key_check").fetchall())
    return results


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Load clean data into SQLite")
    parser.add_argument("--fresh", action="store_true", help="delete the database before loading")
    parser.add_argument("--idempotency-test", action="store_true",
                        help="run the load twice and compare counts + checksums")
    args = parser.parse_args(argv)

    if not config.CLEAN_PATH.exists():
        log.error("data/clean_data.csv not found - run the cleaner first")
        return 1
    df = pd.read_csv(config.CLEAN_PATH)
    if args.fresh and config.DB_PATH.exists():
        config.DB_PATH.unlink()

    conn = connect()
    try:
        conn.executescript(SCHEMA_SQL)
        first = load_dataframe(conn, df)
        checksum_1 = table_checksum(conn)
        log.info("LOAD #1: attempted=%d inserted=%d total_in_db=%d",
                 first["rows_attempted"], first["rows_inserted"], first["rows_after"])
        payload = {"db_path": "data/output.db", "records_processed": len(df),
                   "records_stored": first["rows_after"], "load_1": first, "checksum_1": checksum_1}

        if args.idempotency_test:
            second = load_dataframe(conn, df)
            checksum_2 = table_checksum(conn)
            passed = (second["rows_after"] == first["rows_after"] and second["rows_inserted"] == 0
                      and checksum_1 == checksum_2)
            log.info("LOAD #2: attempted=%d inserted=%d total_in_db=%d", second["rows_attempted"],
                     second["rows_inserted"], second["rows_after"])
            log.info("IDEMPOTENCY %s (checksum #1 %s... / #2 %s...)", "PASSED" if passed else "FAILED",
                     checksum_1[:12], checksum_2[:12])
            payload.update({"load_2": second, "checksum_2": checksum_2, "idempotency_passed": passed,
                            "records_stored": second["rows_after"]})
        payload["verification"] = run_verification(conn)
        stored = count_rows(conn)
        payload["records_stored"] = stored
    except sqlite3.Error:
        return 1
    finally:
        conn.close()

    update_metrics("sqlite", payload)
    log.info("Processed=%d | stored in SQLite=%d", len(df), stored)
    return 0 if payload.get("idempotency_passed", True) else 1


if __name__ == "__main__":
    sys.exit(main())
