"""Run the complete pipeline:  API -> Kafka producer -> Kafka consumer -> Pandera
-> pandas/NumPy cleaning -> SQLite (loaded twice) -> EDA -> verification.

Usage:
    python main.py                 # full run
    python main.py --skip-api      # reuse data/raw_data.csv
    python main.py --only validator cleaner loader
The Kafka producer and consumer stay separate scripts (src/producer.py, src/consumer.py);
main.py simply starts them one after another in separate processes.
"""
import argparse
import subprocess
import sys

from src.utils import get_logger, reset_metrics

log = get_logger("main")

STAGES = [
    ("api_client", ["-m", "src.api_client"]),
    ("producer", ["-m", "src.producer"]),
    ("consumer", ["-m", "src.consumer"]),
    ("validator", ["-m", "src.validator"]),
    ("cleaner", ["-m", "src.cleaner"]),
    ("loader", ["-m", "src.loader", "--fresh", "--idempotency-test"]),
    ("analysis", ["-m", "src.analysis"]),
    ("verify", ["-m", "src.verify"]),
]


def main() -> int:
    parser = argparse.ArgumentParser(description="SIS2 weather data pipeline")
    parser.add_argument("--skip-api", action="store_true", help="reuse existing data/raw_data.csv")
    parser.add_argument("--only", nargs="+", choices=[s for s, _ in STAGES], help="run only these stages")
    args = parser.parse_args()

    selected = [(n, a) for n, a in STAGES if (not args.only or n in args.only)
                and not (args.skip_api and n == "api_client")]
    if not args.only:
        # with --skip-api the metrics of the earlier API collection are kept
        reset_metrics(keep=("api",) if args.skip_api else ())

    for name, cmd in selected:
        log.info("=" * 20 + f" STAGE: {name} " + "=" * 20)
        result = subprocess.run([sys.executable, *cmd])
        if result.returncode != 0:
            log.error("Stage '%s' failed with exit code %d - pipeline stopped.", name, result.returncode)
            return result.returncode
    log.info("Pipeline finished successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
