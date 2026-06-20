import json
import uuid
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest
from inventory.handler import lambda_handler


@pytest.fixture
def mock_context():
    ctx = MagicMock()
    ctx.aws_request_id = "test-request-id"
    return ctx


@pytest.fixture
def mock_settings():
    with patch("inventory.handler.settings") as mock:
        mock.STATUS_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/123/status.fifo"
        mock.ROLLBACK_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/123/rollback.fifo"
        mock.DYNAMODB_TABLE_NAME = "FlashSaleInventory"
        mock.LOG_LEVEL = "INFO"
        yield mock


@pytest.fixture
def mock_dynamodb_client():
    with patch("inventory.handler.DynamoDBClient") as mock_cls:
        mock_instance = MagicMock()
        mock_cls.return_value = mock_instance
        yield mock_instance


def _valid_payment_success_body(**overrides) -> dict:
    base = {
        "event_type": "PaymentSuccess",
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
        "charged_at": datetime.now(UTC).isoformat(),
        "timestamp": datetime.now(UTC).isoformat(),
    }
    base.update(overrides)
    return base


class TestInventoryHandler:
    def test_poison_message_invalid_json_does_not_crash(self, mock_context, mock_settings, mock_dynamodb_client):
        result = lambda_handler(
            {"Records": [{"messageId": "msg-poison", "body": "not-json{"}]},
            mock_context,
        )

        assert result == {"batchItemFailures": []}
        mock_dynamodb_client.get_reservation.assert_not_called()

    def test_valid_message_processes_successfully(self, mock_context, mock_settings, mock_dynamodb_client):
        mock_dynamodb_client.get_reservation.return_value = None
        mock_dynamodb_client.get_event_metadata.return_value = {"sale_status": "ACTIVE"}
        mock_dynamodb_client.conditional_decrement.return_value = True

        body = _valid_payment_success_body()

        with patch("inventory.handler.reserve_ticket") as mock_reserve:
            from inventory.models import ReservationResult

            mock_reserve.return_value = ReservationResult(success=True)
            result = lambda_handler(
                {"Records": [{"messageId": "msg-1", "body": json.dumps(body)}]},
                mock_context,
            )

        assert result == {"batchItemFailures": []}
        mock_reserve.assert_called_once()

    def test_processing_exception_adds_to_batch_failures(self, mock_context, mock_settings, mock_dynamodb_client):
        body = _valid_payment_success_body()

        with patch("inventory.handler.reserve_ticket", side_effect=Exception("DynamoDB down")):
            result = lambda_handler(
                {"Records": [{"messageId": "msg-fail", "body": json.dumps(body)}]},
                mock_context,
            )

        assert result == {"batchItemFailures": [{"itemIdentifier": "msg-fail"}]}
