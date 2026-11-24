"""Shared helpers: logging, JSON metrics file, small utilities."""
from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any

from src import config

_LOG_FORMAT = "%(asctime)s | %(levelname)-7s | %(name)-11s | %(message)s"


def get_logger(name: str) -> logging.Logger:
    """Console + file logger (logs/pipeline.log). Safe to call repeatedly."""
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(_LOG_FORMAT, "%Y-%m-%d %H:%M:%S")
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(formatter)
    logger.addHandler(console)
    config.LOGS_DIR.mkdir(exist_ok=True)
    file_handler = logging.FileHandler(config.LOGS_DIR / "pipeline.log", encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.propagate = False
    return logger


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def json_default(obj: Any) -> Any:
    """Make numpy / pandas scalars JSON serialisable."""
    if hasattr(obj, "item"):
        return obj.item()
    if hasattr(obj, "isoformat"):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serialisable")


def load_metrics() -> dict:
    if config.METRICS_PATH.exists():
        return json.loads(config.METRICS_PATH.read_text(encoding="utf-8"))
    return {}


def update_metrics(stage: str, payload: dict) -> None:
    """Store the results of one pipeline stage in data/pipeline_metrics.json."""
    config.DATA_DIR.mkdir(exist_ok=True)
    metrics = load_metrics()
    metrics[stage] = payload
    config.METRICS_PATH.write_text(
        json.dumps(metrics, indent=2, ensure_ascii=False, default=json_default), encoding="utf-8"
    )


def reset_metrics(keep: tuple[str, ...] = ()) -> None:
    """Start a new run; stages listed in `keep` retain their previous metrics."""
    old = load_metrics()
    if config.METRICS_PATH.exists():
        config.METRICS_PATH.unlink()
    for stage in keep:
        if stage in old:
            update_metrics(stage, old[stage])
