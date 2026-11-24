# Data dictionary

## Raw / consumed record (JSON message and `raw_data.csv`)

| Field | Type | Kind | Description |
|---|---|---|---|
| record_id | text | identifier | `<city>\|<time>`, unique |
| city | text | categorical | City name (8 cities of Kazakhstan) |
| country | text | categorical | ISO country code (`KZ`) |
| latitude, longitude | float | numerical | Coordinates of the API grid cell |
| time | text | date/time | ISO 8601 `YYYY-MM-DDTHH:MM`, time zone `Asia/Almaty` |
| temperature_2m | float | numerical | Air temperature 2 m above ground, degC |
| relative_humidity_2m | float | numerical | Relative humidity, % |
| wind_speed_10m | float | numerical | Wind speed 10 m above ground, km/h |
| weather_code | int | categorical | WMO weather interpretation code |
| source | text | categorical | `open-meteo` (real) or `synthetic_test` (deliberately invalid test record) |

## Final table `weather_observations` (SQLite) / `clean_data.csv`

| Column | SQLite type | Description |
|---|---|---|
| record_id | TEXT PRIMARY KEY | `<city>\|<time>` |
| city | TEXT, FK -> cities.city | Normalised city name |
| observation_time | TEXT | ISO 8601 hour |
| observation_date | TEXT | `YYYY-MM-DD` |
| hour | INTEGER (0-23) | Hour of day |
| part_of_day | TEXT | Night / Morning / Afternoon / Evening |
| temperature_c | REAL | Temperature, degC |
| humidity_pct | REAL (0-100) | Relative humidity, % |
| wind_speed_kmh | REAL (>= 0) | Wind speed, km/h (missing values imputed with city median) |
| wind_speed_ms | REAL | Wind speed, m/s (NumPy) |
| weather_code | INTEGER | WMO code |
| weather_category | TEXT | Clear, Cloudy, Fog, Drizzle, Rain, Snow, Showers, Thunderstorm |
| weather_description | TEXT | Text description of the WMO code |
| dew_point_c | REAL | Dew point (Magnus formula, NumPy) |
| wind_chill_c | REAL | Wind chill for T <= 10 degC and wind > 4.8 km/h, otherwise equal to temperature |
| temp_zscore | REAL | Temperature z-score within the city |
| comfort_level | TEXT | Extreme cold ... Hot, based on wind chill |
| wind_imputed | INTEGER (0/1) | 1 if wind speed was imputed |

Table `cities`: `city` (PK), `country`, `latitude`, `longitude`.
Constraints: `UNIQUE(city, observation_time)`, CHECK constraints for ranges, foreign keys enabled.

## WMO weather code -> category
0-1 Clear | 2-3 Cloudy | 45, 48 Fog | 51-57 Drizzle | 61-67 Rain | 71-77 Snow | 80-86 Showers | 95-99 Thunderstorm
