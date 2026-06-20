import json
import logging

from shared.logger import ContextLogger, JsonFormatter, get_logger


class TestJsonFormatter:
    def test_format_produces_valid_json(self):
        formatter = JsonFormatter()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="Test message",
            args=(),
            exc_info=None,
        )
        record.service_name = "test-service"
        record.transaction_id = "txn-123"
        record.aws_request_id = "req-abc"

        output = formatter.format(record)
        parsed = json.loads(output)

        assert parsed["message"] == "Test message"
        assert parsed["level"] == "INFO"
        assert parsed["service"] == "test-service"
        assert parsed["transaction_id"] == "txn-123"
        assert parsed["aws_request_id"] == "req-abc"
        assert "timestamp" in parsed

    def test_format_without_optional_fields(self):
        formatter = JsonFormatter()
        record = logging.LogRecord(
            name="test",
            level=logging.WARNING,
            pathname="test.py",
            lineno=42,
            msg="Warning message",
            args=(),
            exc_info=None,
        )

        output = formatter.format(record)
        parsed = json.loads(output)

        assert parsed["message"] == "Warning message"
        assert parsed["level"] == "WARNING"
        assert "service" not in parsed
        assert "transaction_id" not in parsed
        assert "aws_request_id" not in parsed

    def test_format_with_exception(self):
        formatter = JsonFormatter()
        try:
            raise ValueError("Test error")
        except ValueError:
            import sys

            exc_info = sys.exc_info()

        record = logging.LogRecord(
            name="test",
            level=logging.ERROR,
            pathname="test.py",
            lineno=1,
            msg="Error occurred",
            args=(),
            exc_info=exc_info,
        )

        output = formatter.format(record)
        parsed = json.loads(output)

        assert "exception" in parsed
        assert "ValueError" in parsed["exception"]
        assert "Test error" in parsed["exception"]


class TestGetLogger:
    def test_returns_context_logger(self):
        logger = get_logger("test-service")
        assert isinstance(logger, ContextLogger)

    def test_logger_has_handler(self):
        logger = get_logger("test-service-2")
        assert len(logger.logger.handlers) > 0

    def test_logger_with_transaction_id(self):
        logger = get_logger("test-service", transaction_id="txn-xyz")
        assert logger.extra["transaction_id"] == "txn-xyz"

    def test_logger_with_aws_request_id(self):
        logger = get_logger("test-service", aws_request_id="req-123")
        assert logger.extra["aws_request_id"] == "req-123"

    def test_logger_default_log_level(self):
        logger = get_logger("test-service-default")
        assert logger.logger.level == logging.INFO

    def test_logger_custom_log_level(self):
        logger = get_logger("test-service-debug", log_level="DEBUG")
        assert logger.logger.level == logging.DEBUG

    def test_logger_invalid_log_level_falls_back_to_info(self):
        logger = get_logger("test-service-invalid", log_level="INVALID")
        assert logger.logger.level == logging.INFO


class TestContextLogger:
    def test_process_adds_context_to_extra(self):
        logger = get_logger(
            "test-service",
            transaction_id="txn-abc",
            aws_request_id="req-def",
        )
        msg, kwargs = logger.process("Test", {})

        assert "extra" in kwargs
        assert kwargs["extra"]["transaction_id"] == "txn-abc"
        assert kwargs["extra"]["aws_request_id"] == "req-def"
        assert kwargs["extra"]["service_name"] == "test-service"

    def test_process_preserves_existing_extra(self):
        logger = get_logger("test-service", transaction_id="txn-abc")
        msg, kwargs = logger.process("Test", {"extra": {"custom_field": "value"}})

        assert kwargs["extra"]["custom_field"] == "value"
        assert kwargs["extra"]["transaction_id"] == "txn-abc"
