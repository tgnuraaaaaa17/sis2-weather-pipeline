"""Stage 2b - Kafka CONSUMER.

Reads the messages published by the producer run (matched through the 'run_id'
header), deserialises the JSON payloads and writes data/consumed_data.csv.

Termination condition (so that the pipeline can finish):
  1. the expected number of messages of this run has been consumed, OR
  2. no new message arrived for CONSUMER_IDLE_TIMEOUT seconds
     (CONSUMER_FIRST_MSG_TIMEOUT while waiting for the very first message).
"""
from __future__ import annotations

import argparse
import json
import sys
import time

import pandas as pd
from confluent_kafka import Consumer, KafkaError, KafkaException

from src import config
from src.utils import get_logger, update_metrics, utc_now_iso

log = get_logger("consumer")


def read_run_info(run_id: str | None, expected: int | None) -> tuple[str, int | None]:
    """Take run_id / expected message count from the producer's run file."""
    if run_id is None:
        if not config.PRODUCER_RUN_PATH.exists():
            raise FileNotFoundError("data/producer_run.json not found - run the producer first")
        info = json.loads(config.PRODUCER_RUN_PATH.read_text(encoding="utf-8"))
        run_id = info["run_id"]
        expected = info["published"] if expected is None else expected
    return run_id, expected


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Consume weather records from Kafka")
    parser.add_argument("--run-id", default=None, help="producer run id (default: data/producer_run.json)")
    parser.add_argument("--expected", type=int, default=None, help="expected number of messages")
    args = parser.parse_args(argv)

    try:
        run_id, expected = read_run_info(args.run_id, args.expected)
    except FileNotFoundError as exc:
        log.error("%s", exc)
        return 1

    consumer = Consumer({
        "bootstrap.servers": config.KAFKA_BOOTSTRAP,
        "group.id": f"sis2-consumer-{run_id}",   # new group per run -> read from the start
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "client.id": "sis2-weather-consumer",
    })
    try:
        consumer.list_topics(config.KAFKA_TOPIC, timeout=10)
    except KafkaException as exc:
        log.error("Kafka broker unavailable: %s", exc)
        consumer.close()
        return 2
    consumer.subscribe([config.KAFKA_TOPIC])
    log.info("Consuming topic '%s' (run_id=%s, expected=%s)", config.KAFKA_TOPIC, run_id, expected)

    records: list[dict] = []
    errors: list[dict] = []
    skipped_other_runs = 0
    broker_errors = 0
    offsets: dict[int, list[int]] = {}
    started = last_message = time.monotonic()
    got_first = False
    terminated_by = "idle_timeout"

    try:
        while True:
            if expected is not None and len(records) + len(errors) >= expected:
                terminated_by = "expected_count_reached"
                break
            msg = consumer.poll(1.0)
            now = time.monotonic()
            if msg is None:
                limit = config.CONSUMER_IDLE_TIMEOUT if got_first else config.CONSUMER_FIRST_MSG_TIMEOUT
                if now - last_message > limit:
                    break
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                broker_errors += 1
                log.error("Kafka error: %s", msg.error())
                continue

            got_first = True
            last_message = now
            headers = {k: v.decode("utf-8") for k, v in (msg.headers() or [])}
            if headers.get("run_id") != run_id:       # message of an older run
                skipped_other_runs += 1
                continue

            span = offsets.setdefault(msg.partition(), [msg.offset(), msg.offset()])
            span[0], span[1] = min(span[0], msg.offset()), max(span[1], msg.offset())
            try:
                record = json.loads(msg.value().decode("utf-8"))   # JSON deserialisation
                if not isinstance(record, dict):
                    raise ValueError("payload is not a JSON object")
                records.append(record)
            except (UnicodeDecodeError, ValueError) as exc:
                errors.append({"offset": msg.offset(), "error": str(exc),
                               "raw": msg.value()[:200].decode("utf-8", "replace")})
                log.error("Cannot deserialise message at offset %s: %s", msg.offset(), exc)
        consumer.commit(asynchronous=False)
    except KafkaException as exc:
        log.error("Consumer failure: %s", exc)
    finally:
        consumer.close()

    frame = pd.DataFrame(records, columns=config.RECORD_COLUMNS)
    config.DATA_DIR.mkdir(exist_ok=True)
    frame.to_csv(config.CONSUMED_PATH, index=False)
    if errors:
        config.CONSUMER_ERRORS_PATH.write_text(
            "\n".join(json.dumps(e) for e in errors), encoding="utf-8")

    summary = {
        "run_id": run_id, "topic": config.KAFKA_TOPIC, "expected": expected,
        "consumed": len(records), "deserialization_errors": len(errors),
        "skipped_other_runs": skipped_other_runs, "broker_errors": broker_errors,
        "terminated_by": terminated_by, "offsets_per_partition": offsets,
        "duration_s": round(time.monotonic() - started, 2), "finished_at": utc_now_iso(),
    }
    update_metrics("consumer", summary)
    log.info("CONSUMED %d messages (errors=%d, skipped=%d, stop=%s) -> %s",
             len(records), len(errors), skipped_other_runs, terminated_by, config.CONSUMED_PATH.name)
    if expected is not None and len(records) != expected:
        log.warning("Expected %d messages but consumed %d", expected, len(records))
        return 3
    return 0


if __name__ == "__main__":
    sys.exit(main())
