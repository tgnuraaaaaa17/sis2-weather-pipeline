"""Central configuration for the whole pipeline (no secrets, no personal paths)."""
import os
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent


def _load_env_file(path: Path) -> None:
    """Tiny .env reader so that no extra dependency is needed."""
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


_load_env_file(ROOT_DIR / ".env")

# ------------------------------------------------------------------ paths
DATA_DIR = ROOT_DIR / "data"
FIGURES_DIR = ROOT_DIR / "figures"
LOGS_DIR = ROOT_DIR / "logs"
SQL_DIR = ROOT_DIR / "sql"

RAW_JSON_PATH = DATA_DIR / "raw_api_responses.json"   # untouched API responses
RAW_CSV_PATH = DATA_DIR / "raw_data.csv"              # flattened, not cleaned
PRODUCER_RUN_PATH = DATA_DIR / "producer_run.json"
CONSUMED_PATH = DATA_DIR / "consumed_data.csv"
CONSUMER_ERRORS_PATH = DATA_DIR / "consumer_errors.jsonl"
VALID_PATH = DATA_DIR / "validated_valid.csv"
QUARANTINE_PATH = DATA_DIR / "quarantine_invalid.csv"
VALIDATION_ERRORS_PATH = DATA_DIR / "validation_errors.csv"
CLEAN_PATH = DATA_DIR / "clean_data.csv"
DB_PATH = DATA_DIR / "output.db"
METRICS_PATH = DATA_DIR / "pipeline_metrics.json"
SUMMARY_PATH = DATA_DIR / "pipeline_summary.md"
FINDINGS_PATH = DATA_DIR / "eda_findings.md"
VERIFY_SQL_PATH = SQL_DIR / "verification_queries.sql"

# -------------------------------------------------------------------- API
API_URL = "https://api.open-meteo.com/v1/forecast"
API_DOCS_URL = "https://open-meteo.com/en/docs"
HOURLY_VARIABLES = ["temperature_2m", "relative_humidity_2m", "wind_speed_10m", "weather_code"]
FORECAST_DAYS = 3
PAST_DAYS = 1
TIMEZONE = "Asia/Almaty"
API_TIMEOUT = 30
API_RETRIES = 3
MIN_RECORDS = 200
SOURCE_API = "open-meteo"
SOURCE_TEST = "synthetic_test"

CITIES = [
    {"name": "Almaty", "latitude": 43.25, "longitude": 76.95},
    {"name": "Astana", "latitude": 51.17, "longitude": 71.43},
    {"name": "Shymkent", "latitude": 42.32, "longitude": 69.59},
    {"name": "Aktobe", "latitude": 50.28, "longitude": 57.21},
    {"name": "Karaganda", "latitude": 49.80, "longitude": 73.10},
    {"name": "Atyrau", "latitude": 47.11, "longitude": 51.92},
    {"name": "Pavlodar", "latitude": 52.29, "longitude": 76.97},
    {"name": "Aktau", "latitude": 43.65, "longitude": 51.15},
]
COUNTRY = "KZ"

RECORD_COLUMNS = [
    "record_id", "city", "country", "latitude", "longitude", "time",
    "temperature_2m", "relative_humidity_2m", "wind_speed_10m", "weather_code", "source",
]

# WMO weather interpretation codes used by Open-Meteo: code -> (category, description)
WMO_CODES = {
    0: ("Clear", "Clear sky"), 1: ("Clear", "Mainly clear"),
    2: ("Cloudy", "Partly cloudy"), 3: ("Cloudy", "Overcast"),
    45: ("Fog", "Fog"), 48: ("Fog", "Depositing rime fog"),
    51: ("Drizzle", "Light drizzle"), 53: ("Drizzle", "Moderate drizzle"),
    55: ("Drizzle", "Dense drizzle"), 56: ("Drizzle", "Light freezing drizzle"),
    57: ("Drizzle", "Dense freezing drizzle"),
    61: ("Rain", "Slight rain"), 63: ("Rain", "Moderate rain"), 65: ("Rain", "Heavy rain"),
    66: ("Rain", "Light freezing rain"), 67: ("Rain", "Heavy freezing rain"),
    71: ("Snow", "Slight snow fall"), 73: ("Snow", "Moderate snow fall"),
    75: ("Snow", "Heavy snow fall"), 77: ("Snow", "Snow grains"),
    80: ("Showers", "Slight rain showers"), 81: ("Showers", "Moderate rain showers"),
    82: ("Showers", "Violent rain showers"), 85: ("Showers", "Slight snow showers"),
    86: ("Showers", "Heavy snow showers"),
    95: ("Thunderstorm", "Thunderstorm"), 96: ("Thunderstorm", "Thunderstorm, slight hail"),
    99: ("Thunderstorm", "Thunderstorm, heavy hail"),
}

# ------------------------------------------------------- validation ranges
TEMP_RANGE = (-90.0, 60.0)       # degrees Celsius
HUMIDITY_RANGE = (0.0, 100.0)    # percent
WIND_RANGE = (0.0, 250.0)        # km/h
MIN_VALID_DATE = "2020-01-01"

# ------------------------------------------------------------------ Kafka
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "weather_raw")
KAFKA_PARTITIONS = 1             # 1 partition -> strict ordering of messages
KAFKA_REPLICATION = 1
CONSUMER_FIRST_MSG_TIMEOUT = 40  # seconds to wait for the very first message
CONSUMER_IDLE_TIMEOUT = 10       # seconds without new messages -> stop
