from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(handler)
    return logger


def log_event(logger: logging.Logger, payload: dict[str, Any]) -> None:
    payload = dict(payload)
    payload.setdefault("ts", datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"))
    logger.info(json.dumps(payload, ensure_ascii=False))
