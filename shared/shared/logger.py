import json
import logging
import sys
from datetime import UTC, datetime


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        if hasattr(record, "service_name"):
            log_entry["service"] = record.service_name
        if hasattr(record, "transaction_id"):
            log_entry["transaction_id"] = record.transaction_id
        if hasattr(record, "aws_request_id"):
            log_entry["aws_request_id"] = record.aws_request_id

        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_entry, default=str)


class ContextLogger(logging.LoggerAdapter):
    def process(self, msg, kwargs):  # type: ignore[override]
        extra = kwargs.setdefault("extra", {})
        ctx = self.extra or {}
        if "transaction_id" in ctx:
            extra["transaction_id"] = ctx["transaction_id"]
        if "service_name" in ctx:
            extra["service_name"] = ctx["service_name"]
        if "aws_request_id" in ctx:
            extra["aws_request_id"] = ctx["aws_request_id"]
        return msg, kwargs


def get_logger(
    service_name: str,
    transaction_id: str | None = None,
    aws_request_id: str | None = None,
    log_level: str = "INFO",
) -> ContextLogger:
    logger = logging.getLogger(f"flash_sale.{service_name}")

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
        logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))
        logger.propagate = False

    return ContextLogger(
        logger,
        {
            "service_name": service_name,
            "transaction_id": transaction_id,
            "aws_request_id": aws_request_id,
        },
    )
