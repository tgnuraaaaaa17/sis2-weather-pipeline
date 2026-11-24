"""Stage 2a - Kafka PRODUCER.

Reads the raw API records (data/raw_data.csv), serialises every record to JSON
and publishes it to a real Apache Kafka topic. Unless --no-test-records is
given, a few deliberately invalid TEST records are appended so that the Pandera
validation stage has something to detect (they are marked source=synthetic_test).
"""
from __future__ import annotations

import argparse
import json
import sys
import time
import uuid
from datetime import datetime, timezone

import pandas as pd
from confluent_kafka import KafkaError, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from src import config
from src.utils import get_logger, update_metrics, utc_now_iso

log = get_logger("producer")


class DeliveryTracker:
    """Counts delivery confirmations coming back from the broker."""

    def __init__(self) -> None:
        self.delivered = 0
        self.failed = 0
        self.errors: list[str] = []

    def callback(self, err, msg) -> None:
        if err is not None:
            self.failed += 1
            self.errors.append(f"{msg.key()}: {err}")
        else:
            self.delivered += 1


def check_broker(admin: AdminClient) -> None:
    """Raise KafkaException if the broker cannot be reached."""
    admin.list_topics(timeout=10)


def ensure_topic(admin: AdminClient, topic: str) -> None:
    """Create the topic if it does not exist yet."""
    futures = admin.create_topics(
        [NewTopic(topic, num_partitions=config.KAFKA_PARTITIONS,
                  replication_factor=config.KAFKA_REPLICATION)]
    )
    for name, future in futures.items():
        try:
            future.result()
            log.info("Topic '%s' created (partitions=%d)", name, config.KAFKA_PARTITIONS)
        except KafkaException as exc:
            if exc.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                log.info("Topic '%s' already exists - reusing it", name)
            else:
                raise


def load_api_records(path=config.RAW_CSV_PATH) -> list[dict]:
    frame = pd.read_csv(path)
    # to_json writes NaN as null and converts numpy types to plain JSON types
    return json.loads(frame.to_json(orient="records"))


def build_test_records(real_records: list[dict]) -> list[dict]:
    """Five deliberately invalid records (each breaks exactly one rule)."""
    base = real_records[0]
    stem = base["time"][:-2]  # e.g. 2026-11-24T13:  -> unique minute per test record

    def make(minute: str, **override) -> dict:
        record = dict(base, source=config.SOURCE_TEST)
        record["time"] = f"{stem}{minute}"
        record["record_id"] = f"{record['city']}|{record['time']}"
        record.update(override)
        return record

    return [
        make("15", relative_humidity_2m=135.0),          # humidity out of 0-100
        make("30", temperature_2m=None),                 # missing required value
        make("45", weather_code=999),                    # code not in WMO list
        make("59", wind_speed_10m=-50.0),                # negative wind speed
        dict(base, source=config.SOURCE_TEST),           # duplicate record_id
    ]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Publish API records to Kafka")
    parser.add_argument("--input", default=str(config.RAW_CSV_PATH))
    parser.add_argument("--no-test-records", action="store_true",
                        help="do not append deliberately invalid test records")
    args = parser.parse_args(argv)

    try:
        api_records = load_api_records(args.input)
    except (FileNotFoundError, pd.errors.EmptyDataError) as exc:
        log.error("Cannot read input data (%s). Run src/api_client.py first.", exc)
        return 1
    test_records = [] if args.no_test_records else build_test_records(api_records)
    to_send = api_records + test_records

    log.info("Connecting to Kafka broker at %s", config.KAFKA_BOOTSTRAP)
    admin = AdminClient({"bootstrap.servers": config.KAFKA_BOOTSTRAP})
    try:
        check_broker(admin)
        ensure_topic(admin, config.KAFKA_TOPIC)
    except KafkaException as exc:
        log.error("Kafka broker unavailable: %s. Is 'docker compose up -d' running?", exc)
        return 2

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:6]
    tracker = DeliveryTracker()
    producer = Producer({
        "bootstrap.servers": config.KAFKA_BOOTSTRAP,
        "client.id": "sis2-weather-producer",
        "acks": "all",
        "enable.idempotence": True,
        "message.timeout.ms": 30000,
    })

    started = time.monotonic()
    serialization_errors = 0
    queued = 0
    for record in to_send:
        try:
            payload = json.dumps(record, ensure_ascii=False, allow_nan=False).encode("utf-8")
        except (TypeError, ValueError) as exc:
            serialization_errors += 1
            log.error("JSON serialisation failed for %s: %s", record.get("record_id"), exc)
            continue
        while True:
            try:
                producer.produce(
                    config.KAFKA_TOPIC, key=str(record["record_id"]).encode("utf-8"),
                    value=payload, headers=[("run_id", run_id.encode("utf-8"))],
                    callback=tracker.callback,
                )
                queued += 1
                break
            except BufferError:      # local queue full -> let librdkafka drain it
                producer.poll(1.0)
            except KafkaException as exc:
                log.error("Transmission error for %s: %s", record.get("record_id"), exc)
                tracker.failed += 1
                break
        producer.poll(0)

    remaining = producer.flush(30)
    if remaining:
        log.error("%d messages were not delivered before the flush timeout", remaining)
        tracker.failed += remaining

    duration = round(time.monotonic() - started, 2)
    summary = {
        "run_id": run_id, "topic": config.KAFKA_TOPIC, "broker": config.KAFKA_BOOTSTRAP,
        "partitions": config.KAFKA_PARTITIONS,
        "api_records_sent": len(api_records), "test_records_sent": len(test_records),
        "messages_queued": queued, "published": tracker.delivered,
        "delivery_failed": tracker.failed, "serialization_errors": serialization_errors,
        "duration_s": duration, "finished_at": utc_now_iso(),
    }
    config.PRODUCER_RUN_PATH.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    update_metrics("producer", summary)
    log.info("PUBLISHED %d messages (failed=%d) to topic '%s' | run_id=%s",
             tracker.delivered, tracker.failed, config.KAFKA_TOPIC, run_id)
    for line in tracker.errors[:5]:
        log.error("delivery error: %s", line)
    return 0 if tracker.failed == 0 and serialization_errors == 0 else 3


if __name__ == "__main__":
    sys.exit(main())
