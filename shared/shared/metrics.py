import logging
import threading

import boto3

logger = logging.getLogger(__name__)

_cloudwatch_client = None
_metric_buffer: list[dict] = []
_buffer_lock = threading.Lock()
BUFFER_SIZE = 20
NAMESPACE = "FlashSaleSaga"


def _get_cloudwatch_client():
    global _cloudwatch_client
    if _cloudwatch_client is None:
        _cloudwatch_client = boto3.client("cloudwatch")
    return _cloudwatch_client


def emit_metric(
    name: str,
    value: float = 1.0,
    unit: str = "Count",
    dimensions: dict[str, str] | None = None,
) -> None:
    metric_data = {
        "MetricName": name,
        "Value": value,
        "Unit": unit,
    }

    if dimensions:
        metric_data["Dimensions"] = [{"Name": k, "Value": v} for k, v in dimensions.items()]

    should_flush = False
    with _buffer_lock:
        _metric_buffer.append(metric_data)
        if len(_metric_buffer) >= BUFFER_SIZE:
            should_flush = True

    if should_flush:
        flush_metrics()


def flush_metrics() -> None:
    global _metric_buffer

    with _buffer_lock:
        if not _metric_buffer:
            return
        batch = _metric_buffer
        _metric_buffer = []

    client = _get_cloudwatch_client()

    try:
        client.put_metric_data(
            Namespace=NAMESPACE,
            MetricData=batch,
        )
        logger.debug("Flushed %d metrics to CloudWatch", len(batch))
    except Exception as e:
        logger.error("Failed to flush metrics to CloudWatch: %s", e)


def set_cloudwatch_client(client) -> None:
    global _cloudwatch_client
    _cloudwatch_client = client


def clear_metric_buffer() -> None:
    global _metric_buffer
    with _buffer_lock:
        _metric_buffer = []
