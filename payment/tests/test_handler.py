import json
import uuid
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest
from payment.handler import lambda_handler


def _valid_process_payment_body(**overrides) -> dict:
    base = {
        "event_type": "ProcessPayment",
        "version": "1.0",
        "transaction_id": str(uuid.uuid4()),
        "idempotency_key": str(uuid.uuid4()),
        "user_id": str(uuid.uuid4()),
        "event_id": str(uuid.uuid4()),
        "tier_name": "VIP",
        "quantity": 1,
        "amount_cents": 4999,
        "currency": "USD",
        "timestamp": datetime.now(UTC).isoformat(),
    }
    base.update(overrides)
    return base


def _valid_inventory_failed_body(**overrides) -> dict:
    base = {
        "event_type": "InventoryFailed",
        "version": "1.0",
        "transaction_id": str(uuid.uuid4()),
        "idempotency_key": str(uuid.uuid4()),
        "user_id": str(uuid.uuid4()),
        "event_id": str(uuid.uuid4()),
        "tier_name": "VIP",
        "quantity": 1,
        "amount_cents": 4999,
        "currency": "USD",
        "payment_reference": str(uuid.uuid4()),
        "failure_reason": "SOLD_OUT",
        "failure_code": "INVENTORY_EXHAUSTED",
        "failed_at": datetime.now(UTC).isoformat(),
        "timestamp": datetime.now(UTC).isoformat(),
    }
    base.update(overrides)
    return base


@pytest.fixture
def mock_context():
    ctx = MagicMock()
    ctx.aws_request_id = "test-request-id"
    return ctx


@pytest.fixture
def mock_settings():
    with patch("payment.handler.settings") as mock:
        mock.DATABASE_URL = "postgresql://test:test@localhost/test"
        mock.INVENTORY_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/123/inventory.fifo"
        mock.STATUS_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/123/status.fifo"
        mock.LOG_LEVEL = "INFO"
        yield mock


@pytest.fixture
def mock_db_connection():
    with (
        patch("payment.handler.get_connection") as mock_get,
        patch("payment.handler.release_connection") as mock_release,
    ):
        mock_conn = MagicMock()
        mock_get.return_value = mock_conn
        yield mock_conn, mock_get, mock_release


class TestPaymentHandler:
    def test_routes_process_payment(self, mock_context, mock_settings, mock_db_connection, mock_sqs_client=None):
        conn, mock_get, mock_release = mock_db_connection
        body = _valid_process_payment_body()

        with patch("payment.handler.process_payment", return_value=MagicMock(success=True)) as mock_proc:
            result = lambda_handler(
                {"Records": [{"messageId": "msg-1", "body": json.dumps(body)}]},
                mock_context,
            )

        assert result == {"batchItemFailures": []}
        mock_proc.assert_called_once()
        mock_release.assert_called_once_with(conn, mock_settings.DATABASE_URL)

    def test_routes_inventory_failed(self, mock_context, mock_settings, mock_db_connection):
        conn, mock_get, mock_release = mock_db_connection
        body = _valid_inventory_failed_body()

        with patch("payment.handler.process_rollback", return_value=MagicMock(success=True)) as mock_rb:
            result = lambda_handler(
                {"Records": [{"messageId": "msg-1", "body": json.dumps(body)}]},
                mock_context,
            )

        assert result == {"batchItemFailures": []}
        mock_rb.assert_called_once()

    def test_poison_message_invalid_json_does_not_retry(self, mock_context, mock_settings, mock_db_connection):
        result = lambda_handler(
            {"Records": [{"messageId": "msg-poison", "body": "not-json{"}]},
            mock_context,
        )

        assert result == {"batchItemFailures": []}

    def test_unknown_event_type_does_not_retry(self, mock_context, mock_settings, mock_db_connection):
        body = json.dumps({"event_type": "UnknownEvent", "version": "1.0"})

        result = lambda_handler(
            {"Records": [{"messageId": "msg-1", "body": body}]},
            mock_context,
        )

        assert result == {"batchItemFailures": []}

    def test_processing_exception_adds_to_batch_failures(self, mock_context, mock_settings, mock_db_connection):
        body = _valid_process_payment_body()

        with patch("payment.handler.process_payment", side_effect=Exception("DB down")):
            result = lambda_handler(
                {"Records": [{"messageId": "msg-fail", "body": json.dumps(body)}]},
                mock_context,
            )

        assert result == {"batchItemFailures": [{"itemIdentifier": "msg-fail"}]}
