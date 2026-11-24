-- Verification queries executed by src/loader.py (each block starts with "-- name: <id>").

-- name: total_rows
SELECT COUNT(*) AS total_rows FROM weather_observations;

-- name: rows_per_city
SELECT city, COUNT(*) AS rows, ROUND(AVG(temperature_c), 2) AS avg_temp_c
FROM weather_observations GROUP BY city ORDER BY city;

-- name: time_range
SELECT MIN(observation_time) AS first_hour, MAX(observation_time) AS last_hour FROM weather_observations;

-- name: duplicate_check
SELECT record_id, COUNT(*) AS copies FROM weather_observations GROUP BY record_id HAVING COUNT(*) > 1;

-- name: null_check
SELECT SUM(temperature_c IS NULL) AS null_temp, SUM(humidity_pct IS NULL) AS null_humidity,
       SUM(weather_category IS NULL) AS null_category
FROM weather_observations;

-- name: weather_categories
SELECT weather_category, COUNT(*) AS hours FROM weather_observations GROUP BY weather_category ORDER BY hours DESC;

-- name: top5_coldest_hours
SELECT city, observation_time, temperature_c FROM weather_observations ORDER BY temperature_c ASC LIMIT 5;

-- name: orphan_check
SELECT COUNT(*) AS rows_without_city FROM weather_observations o
LEFT JOIN cities c ON c.city = o.city WHERE c.city IS NULL;
