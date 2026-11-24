# SIS2 - Weather Data Streaming and Validation Pipeline

End-to-end pipeline for hourly weather data of 8 Kazakhstan cities:

```
Open-Meteo API -> Kafka producer -> Kafka topic -> Kafka consumer -> Pandera validation
              -> pandas/NumPy cleaning -> SQLite3 (idempotent) -> EDA + figures -> verification
```

* **API:** Open-Meteo Forecast API (`https://api.open-meteo.com/v1/forecast`), no API key, no credentials.
* **Dataset:** 8 cities x 96 hourly rows (1 past day + 3 forecast days) = **768 records**
  - numerical: `temperature_2m`, `relative_humidity_2m`, `wind_speed_10m`
  - categorical: `weather_code` (WMO), `city`
  - date/time: `time`
* **Broker:** real Apache Kafka 3.8 (Docker, KRaft). No simulated Kafka.

## 1. Requirements

* Python 3.10 - 3.12
* Docker + Docker Compose (for the Kafka broker)
* Internet access (Open-Meteo API, Docker image, pip packages)

## 2. Installation

```bash
git clone <your-repository-url>
cd sis2_weather_pipeline
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

Optional: `cp .env.example .env` to change `KAFKA_BOOTSTRAP_SERVERS` / `KAFKA_TOPIC`
(defaults `localhost:9092` / `weather_raw`). There are no secrets in this project.

## 3. Start the Kafka broker

```bash
docker compose up -d
docker compose ps                  # wait until the container is "running" (~10-15 s)
```

Topic configuration: name `weather_raw`, 1 partition, replication factor 1. The producer creates it
automatically; to create it manually:

```bash
docker exec sis2-kafka /opt/kafka/bin/kafka-topics.sh --create --topic weather_raw \
  --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

## 4. Run the complete pipeline

```bash
python main.py
```

`main.py` runs the stages one after another (each stage is also runnable alone):

| # | Command | Result |
|---|---------|--------|
| 1 | `python -m src.api_client` | `data/raw_api_responses.json`, `data/raw_data.csv` |
| 2 | `python -m src.producer` | publishes every record (+5 invalid test records) to Kafka |
| 3 | `python -m src.consumer` | `data/consumed_data.csv` |
| 4 | `python -m src.validator` | `data/validated_valid.csv`, `data/quarantine_invalid.csv`, `data/validation_errors.csv` |
| 5 | `python -m src.cleaner` | `data/clean_data.csv`, summary tables |
| 6 | `python -m src.loader --fresh --idempotency-test` | `data/output.db` (loaded twice) |
| 7 | `python -m src.analysis` | `figures/*.png`, `data/eda_findings.md` |
| 8 | `python -m src.verify` | `data/pipeline_summary.md` (counts + PASS/FAIL checks) |

Producer and consumer as separate scripts (two terminals are also possible - start the consumer after the producer
has written `data/producer_run.json`):

```bash
python -m src.producer
python -m src.consumer
```

Useful options: `python main.py --skip-api` (reuse `data/raw_data.csv`), `python main.py --only validator cleaner`,
`python -m src.producer --no-test-records`.

**Consumer termination:** it stops when all messages of the producer run were consumed, or after
10 s without messages (40 s for the first message).

## 5. Verification of a successful run

* Console ends with `PIPELINE VERIFIED - all checks passed`; see `data/pipeline_summary.md`.
* Counts to compare: API records, published, consumed, valid, invalid, cleaned, stored in SQLite.
* Check Kafka offsets: `docker exec sis2-kafka /opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --bootstrap-server localhost:9092 --topic weather_raw`
* SQL checks (queries in `sql/verification_queries.sql`, executed by the loader):

```bash
sqlite3 data/output.db "SELECT COUNT(*) FROM weather_observations;"
sqlite3 data/output.db "SELECT record_id, COUNT(*) FROM weather_observations GROUP BY record_id HAVING COUNT(*) > 1;"
```
* Idempotency: the loader log shows `LOAD #2 ... inserted=0` and `IDEMPOTENCY PASSED`.
  You can also run `python -m src.loader` again - the row count and table checksum stay the same.
* Validation tests: `python -m pytest tests -q`

## 6. Expected outputs

`data/` (CSV, JSON, `output.db`), `figures/` (6 PNG files), `logs/pipeline.log`, `data/pipeline_metrics.json`.

## 7. Project report (PDF)

1. Review `report/report_config.json` (title, date, members, contributions). The report omits the group number because it was not available.
2. Run `python main.py` successfully.
3. `python report/build_report.py` -> `report/project_report.pdf`
   (all numbers, tables, figures, log excerpts and code excerpts come from the real run).
4. Add the section "Encountered errors" (9.3) from your own experience if needed.

## 8. Repository structure

```
main.py  requirements.txt  docker-compose.yml  .env.example
src/   config.py utils.py api_client.py producer.py consumer.py validator.py cleaner.py loader.py analysis.py verify.py
sql/   verification_queries.sql
tests/ test_validation.py
data/  raw_data.csv  output.db  ...        figures/  docs/data_dictionary.md  report/  logs/
```

## 9. Notes

* Forecast values of the same hour change between API calls. Re-running the pipeline later updates existing rows
  (UPSERT on `record_id`) and adds new hours; it never creates duplicates.
* To start with an empty topic: `docker compose down -v && docker compose up -d`.
