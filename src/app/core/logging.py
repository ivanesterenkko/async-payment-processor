from __future__ import annotations

import json
import logging
from datetime import UTC, date, datetime
from decimal import Decimal
from uuid import UUID

STANDARD_LOG_RECORD_FIELDS = {
    "args",
    "asctime",
    "created",
    "exc_info",
    "exc_text",
    "filename",
    "funcName",
    "levelname",
    "levelno",
    "lineno",
    "module",
    "msecs",
    "message",
    "msg",
    "name",
    "pathname",
    "process",
    "processName",
    "relativeCreated",
    "stack_info",
    "thread",
    "threadName",
    "taskName",
}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, object] = {
            "timestamp": datetime.now(UTC).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        for key, value in record.__dict__.items():
            if key not in STANDARD_LOG_RECORD_FIELDS and not key.startswith("_"):
                payload[key] = self._serialize(value)

        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)

        return json.dumps(payload, ensure_ascii=True, sort_keys=True)

    @staticmethod
    def _serialize(value: object) -> object:
        if isinstance(value, (UUID, datetime, date, Decimal)):
            return str(value)
        return value


def configure_logging(level: str) -> None:
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    root_logger.handlers = [handler]


def log_context(**fields: object) -> dict[str, object]:
    return fields
