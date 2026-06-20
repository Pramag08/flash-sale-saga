import json
import uuid
from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError
from inventory.reserver import reserve_ticket
from shared.events import PaymentSuccessEvent


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _valid_payment_success_message(**overrides) -> PaymentSuccessEvent:
    base = {
        "event_type": "PaymentSuccess",
        "version": "1.0",
        "transaction_id": str(uuid.uuid4()),
        "idempotency_key": str(uuid.uuid4()),
        "user_id": str(uuid.uuid4()),
        "event_id": str(uuid.uuid4()),
        "tier_name": "VIP",
        "quantity": 2,
        "amount_cents": 9998,
        "currency": "USD",
        "payment_reference": str(uuid.uuid4()),
        "charged_at": _utc_now().isoformat(),
        "timestamp": _utc_now().isoformat(),
    }
    base.update(overrides)
    return PaymentSuccessEvent.model_validate(base)


@pytest.fixture
def mock_db():
    db = MagicMock()
    db.find_active_reservation.return_value = []
    db.put_reservation.return_value = True
    return db


@pytest.fixture
def mock_sqs_client():
    client = MagicMock()
    client.send_message.return_value = {"MessageId": "msg-123"}
    return client


class TestReserveTicket:
    def test_happy_path(self, mock_db, mock_sqs_client):
        message = _valid_payment_success_message()
        mock_db.get_reservation.return_value = None
        mock_db.get_event_metadata.return_value = {"sale_status": "ACTIVE"}
        mock_db.conditional_decrement.return_value = True

        result = reserve_ticket(
            message=message,
            db=mock_db,
            status_queue_url="https://sqs.us-east-1.amazonaws.com/123/status.fifo",
            rollback_queue_url="https://sqs.us-east-1.amazonaws.com/123/rollback.fifo",
            sqs_client=mock_sqs_client,
        )

        assert result.success is True
        assert result.failure_code is None
        assert result.failure_reason is None

        mock_db.conditional_decrement.assert_called_once()
        mock_db.put_reservation.assert_called_once()
        assert mock_sqs_client.send_message.call_count == 1

        call_kwargs = mock_sqs_client.send_message.call_args[1]
        assert call_kwargs["QueueUrl"] == "https://sqs.us-east-1.amazonaws.com/123/status.fifo"
        body = json.loads(call_kwargs["MessageBody"])
        assert body["event_type"] == "SagaCompleted"
        assert body["outcome"] == "SUCCESS"

    def test_duplicate_reservation_already_reserved_re_emits_saga_completed(self, mock_db, mock_sqs_client):
        message = _valid_payment_success_message()
        mock_db.get_reservation.return_value = {"status": "RESERVED"}

        result = reserve_ticket(
            message=message,
            db=mock_db,
            status_queue_url="https://sqs.us-east-1.amazonaws.com/123/status.fifo",
            rollback_queue_url="https://sqs.us-east-1.amazonaws.com/123/rollback.fifo",
            sqs_client=mock_sqs_client,
        )

        assert result.success is True
        mock_db.conditional_decrement.assert_not_called()
        mock_db.put_reservation.assert_not_called()
        mock_sqs_client.send_message.assert_called_once()

        call_kwargs = mock_sqs_client.send_message.call_args[1]
        assert call_kwargs["QueueUrl"] == "https://sqs.us-east-1.amazonaws.com/123/status.fifo"
        body = json.loads(call_kwargs["MessageBody"])
        assert body["event_type"] == "SagaCompleted"
        assert body["outcome"] == "SUCCESS"

    def test_duplicate_reservation_already_confirmed_re_emits_saga_completed(self, mock_db, mock_sqs_client):
        message = _valid_payment_success_message()
        mock_db.get_reservation.return_value = {"status": "CONFIRMED"}

        result = reserve_ticket(
            message=message,
            db=mock_db,
            status_queue_url="https://sqs.us-east-1.amazonaws.com/123/status.fifo",
            rollback_queue_url="https://sqs.us-east-1.amazonaws.com/123/rollback.fifo",
            sqs_client=mock_sqs_client,
        )

        assert result.success is True
        mock_db.conditional_decrement.assert_not_called()
        mock_sqs_client.send_message.assert_called_once()

        call_kwargs = mock_sqs_client.send_message.call_args[1]
        body = json.loads(call_kwargs["MessageBody"])
        assert body["event_type"] == "SagaCompleted"
        assert body["outcome"] == "SUCCESS"

    def test_event_not_found(self, mock_db, mock_sqs_client):
        message = _valid_payment_success_message()
        mock_db.get_reservation.return_value = None
        mock_db.get_event_metadata.return_value = None

        result = reserve_ticket(
            message=message,
            db=mock_db,
            status_queue_url="https://sqs.us-east-1.amazonaws.com/123/status.fifo",
            rollback_queue_url="https://sqs.us-east-1.amazonaws.com/123/rollback.fifo",
            sqs_client=mock_sqs_client,
        )

        assert result.success is False
        assert result.failure_code == "EVENT_NOT_FOUND"
        assert result.failure_reason == "EVENT_NOT_FOUND"
        mock_db.conditional_decrement.assert_not_called()
        assert mock_sqs_client.send_message.call_count == 1

        call_kwargs = mock_sqs_client.send_message.call_args[1]
        assert call_kwargs["QueueUrl"] == "https://sqs.us-east-1.amazonaws.com/123/rollback.fifo"
        body = json.loads(call_kwargs["MessageBody"])
        assert body["event_type"] == "InventoryFailed"
        assert body["failure_code"] == "EVENT_NOT_FOUND"

    def test_event_closed(self, mock_db, mock_sqs_client):
        message = _valid_payment_success_message()
        mock_db.get_reservation.return_value = None
        mock_db.get_event_metadata.return_value = {"sale_status": "CLOSED"}

        result = reserve_ticket(
            message=message,
            db=mock_db,
            status_queue_url="https://sqs.us-east-1.amazonaws.com/123/status.fifo",
            rollback_queue_url="https://sqs.us-east-1.amazonaws.com/123/rollback.fifo",
            sqs_client=mock_sqs_client,
        )

        assert result.success is False
        assert result.failure_code == "EVENT_CLOSED"
        assert result.failure_reason == "EVENT_CLOSED"
        mock_db.conditional_decrement.assert_not_called()
        assert mock_sqs_client.send_message.call_count == 1

    def test_inventory_exhausted(self, mock_db, mock_sqs_client):
        message = _valid_payment_success_message()
        mock_db.get_reservation.return_value = None
        mock_db.get_event_metadata.return_value = {"sale_status": "ACTIVE"}
        mock_db.conditional_decrement.return_value = False

        result = reserve_ticket(
            message=message,
            db=mock_db,
            status_queue_url="https://sqs.us-east-1.amazonaws.com/123/status.fifo",
            rollback_queue_url="https://sqs.us-east-1.amazonaws.com/123/rollback.fifo",
            sqs_client=mock_sqs_client,
        )

        assert result.success is False
        assert result.failure_code == "INVENTORY_EXHAUSTED"
        assert result.failure_reason == "SOLD_OUT"
        mock_db.put_reservation.assert_not_called()
        assert mock_sqs_client.send_message.call_count == 1

        call_kwargs = mock_sqs_client.send_message.call_args[1]
        assert call_kwargs["QueueUrl"] == "https://sqs.us-east-1.amazonaws.com/123/rollback.fifo"
        body = json.loads(call_kwargs["MessageBody"])
        assert body["event_type"] == "InventoryFailed"
        assert body["failure_code"] == "INVENTORY_EXHAUSTED"
        assert body["failure_reason"] == "SOLD_OUT"

    def test_max_per_user_exceeded_backstop(self, mock_db, mock_sqs_client):
        message = _valid_payment_success_message()
        mock_db.get_reservation.return_value = None
        mock_db.get_event_metadata.return_value = {"sale_status": "ACTIVE"}
        mock_db.find_active_reservation.return_value = [{"PK": "TXN#other-txn", "status": "RESERVED"}]

        result = reserve_ticket(
            message=message,
            db=mock_db,
            status_queue_url="https://sqs.us-east-1.amazonaws.com/123/status.fifo",
            rollback_queue_url="https://sqs.us-east-1.amazonaws.com/123/rollback.fifo",
            sqs_client=mock_sqs_client,
        )

        assert result.success is False
        assert result.failure_code == "MAX_PER_USER_EXCEEDED"
        assert result.failure_reason == "MAX_PER_USER_EXCEEDED"
        mock_db.conditional_decrement.assert_not_called()
        mock_db.put_reservation.assert_not_called()
        assert mock_sqs_client.send_message.call_count == 1

        call_kwargs = mock_sqs_client.send_message.call_args[1]
        assert call_kwargs["QueueUrl"] == "https://sqs.us-east-1.amazonaws.com/123/rollback.fifo"
        body = json.loads(call_kwargs["MessageBody"])
        assert body["event_type"] == "InventoryFailed"
        assert body["failure_code"] == "MAX_PER_USER_EXCEEDED"

    def test_conditional_put_race_restores_quantity_and_re_emits(self, mock_db, mock_sqs_client):
        message = _valid_payment_success_message()
        mock_db.get_reservation.return_value = None
        mock_db.get_event_metadata.return_value = {"sale_status": "ACTIVE"}
        mock_db.conditional_decrement.return_value = True
        mock_db.put_reservation.return_value = False

        result = reserve_ticket(
            message=message,
            db=mock_db,
            status_queue_url="https://sqs.us-east-1.amazonaws.com/123/status.fifo",
            rollback_queue_url="https://sqs.us-east-1.amazonaws.com/123/rollback.fifo",
            sqs_client=mock_sqs_client,
        )

        assert result.success is True
        mock_db.restore_quantity.assert_called_once()
        assert mock_sqs_client.send_message.call_count == 1

        call_kwargs = mock_sqs_client.send_message.call_args[1]
        assert call_kwargs["QueueUrl"] == "https://sqs.us-east-1.amazonaws.com/123/status.fifo"
        body = json.loads(call_kwargs["MessageBody"])
        assert body["event_type"] == "SagaCompleted"
        assert body["outcome"] == "SUCCESS"

    def test_event_upcoming(self, mock_db, mock_sqs_client):
        message = _valid_payment_success_message()
        mock_db.get_reservation.return_value = None
        mock_db.get_event_metadata.return_value = {"sale_status": "UPCOMING"}

        result = reserve_ticket(
            message=message,
            db=mock_db,
            status_queue_url="https://sqs.us-east-1.amazonaws.com/123/status.fifo",
            rollback_queue_url="https://sqs.us-east-1.amazonaws.com/123/rollback.fifo",
            sqs_client=mock_sqs_client,
        )

        assert result.success is False
        assert result.failure_code == "EVENT_CLOSED"

    def test_reservation_created_with_correct_params(self, mock_db, mock_sqs_client):
        message = _valid_payment_success_message()
        mock_db.get_reservation.return_value = None
        mock_db.get_event_metadata.return_value = {"sale_status": "ACTIVE"}
        mock_db.conditional_decrement.return_value = True

        reserve_ticket(
            message=message,
            db=mock_db,
            status_queue_url="https://sqs.us-east-1.amazonaws.com/123/status.fifo",
            rollback_queue_url="https://sqs.us-east-1.amazonaws.com/123/rollback.fifo",
            sqs_client=mock_sqs_client,
        )

        mock_db.put_reservation.assert_called_once_with(
            transaction_id=str(message.transaction_id),
            event_id=str(message.event_id),
            tier_name=message.tier_name,
            user_id=str(message.user_id),
            quantity=message.quantity,
            payment_reference=str(message.payment_reference),
        )

    def test_dynamodb_table_not_found_emits_rollback(self, mock_db, mock_sqs_client):
        message = _valid_payment_success_message()
        mock_db.table_name = "FlashSaleInventory"
        mock_db.get_reservation.side_effect = ClientError(
            {"Error": {"Code": "ResourceNotFoundException", "Message": "Table not found"}},
            "GetItem",
        )

        result = reserve_ticket(
            message=message,
            db=mock_db,
            status_queue_url="https://sqs.us-east-1.amazonaws.com/123/status.fifo",
            rollback_queue_url="https://sqs.us-east-1.amazonaws.com/123/rollback.fifo",
            sqs_client=mock_sqs_client,
        )

        assert result.success is False
        assert result.failure_code == "INTERNAL_ERROR"
        assert result.failure_reason == "INTERNAL_ERROR"
        mock_db.conditional_decrement.assert_not_called()
        mock_db.put_reservation.assert_not_called()
        assert mock_sqs_client.send_message.call_count == 1

        call_kwargs = mock_sqs_client.send_message.call_args[1]
        assert call_kwargs["QueueUrl"] == "https://sqs.us-east-1.amazonaws.com/123/rollback.fifo"
        body = json.loads(call_kwargs["MessageBody"])
        assert body["event_type"] == "InventoryFailed"
        assert body["failure_code"] == "INTERNAL_ERROR"
        assert body["failure_reason"] == "INTERNAL_ERROR"

    def test_non_resource_client_error_reraises(self, mock_db, mock_sqs_client):
        message = _valid_payment_success_message()
        mock_db.get_reservation.side_effect = ClientError(
            {"Error": {"Code": "ProvisionedThroughputExceededException", "Message": "throttled"}},
            "GetItem",
        )

        with pytest.raises(ClientError):
            reserve_ticket(
                message=message,
                db=mock_db,
                status_queue_url="https://sqs.us-east-1.amazonaws.com/123/status.fifo",
                rollback_queue_url="https://sqs.us-east-1.amazonaws.com/123/rollback.fifo",
                sqs_client=mock_sqs_client,
            )

        mock_sqs_client.send_message.assert_not_called()
