import json
import uuid
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import psycopg2
import pytest
from payment.rollback import process_rollback
from shared.events import InventoryFailedEvent
from shared.exceptions import SagaError


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _valid_inventory_failed_message(**overrides) -> InventoryFailedEvent:
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
        "failed_at": _utc_now().isoformat(),
        "timestamp": _utc_now().isoformat(),
    }
    base.update(overrides)
    return InventoryFailedEvent.model_validate(base)


@pytest.fixture
def mock_db():
    db = MagicMock()
    cursor = MagicMock()
    db.cursor.return_value = cursor
    db.set_isolation_level = MagicMock()
    db.autocommit = False
    db.commit = MagicMock()
    db.rollback = MagicMock()
    return db, cursor


@pytest.fixture
def mock_sqs_client():
    client = MagicMock()
    client.send_message.return_value = {"MessageId": "msg-123"}
    return client


class TestProcessRollback:
    def test_happy_path(self, mock_db, mock_sqs_client):
        db, cursor = mock_db
        message = _valid_inventory_failed_message()
        refund_ledger_id = uuid.uuid4()
        original_charge_id = uuid.uuid4()
        user_id = str(uuid.uuid4())

        cursor.fetchone.side_effect = [
            (str(message.idempotency_key),),
            (str(original_charge_id), 4999, user_id, "SUCCESS"),
            (10000,),
            (refund_ledger_id,),
            None,
        ]

        result = process_rollback(
            message=message,
            db=db,
            status_queue_url="https://sqs.us-east-1.amazonaws.com/123/status.fifo",
            sqs_client=mock_sqs_client,
        )

        assert result.success is True
        assert result.refund_ledger_id == refund_ledger_id
        assert result.error is None

        db.set_isolation_level.assert_called_once()
        assert db.commit.call_count >= 2
        mock_sqs_client.send_message.assert_called_once()

    def test_duplicate_rollback_already_completed(self, mock_db, mock_sqs_client):
        db, cursor = mock_db
        message = _valid_inventory_failed_message()
        refund_ledger_id = uuid.uuid4()

        cursor.fetchone.side_effect = [
            None,
            ("COMPLETED", json.dumps({"refund_ledger_id": str(refund_ledger_id), "success": True}), _utc_now()),
        ]

        result = process_rollback(
            message=message,
            db=db,
            status_queue_url="https://sqs.us-east-1.amazonaws.com/123/status.fifo",
            sqs_client=mock_sqs_client,
        )

        assert result.success is True
        assert result.refund_ledger_id == refund_ledger_id
        mock_sqs_client.send_message.assert_not_called()

    def test_original_charge_not_found(self, mock_db, mock_sqs_client):
        db, cursor = mock_db
        message = _valid_inventory_failed_message()

        cursor.fetchone.side_effect = [
            (str(message.idempotency_key),),
            None,
        ]

        result = process_rollback(
            message=message,
            db=db,
            status_queue_url="https://sqs.us-east-1.amazonaws.com/123/status.fifo",
            sqs_client=mock_sqs_client,
        )

        assert result.success is False
        assert result.error == "ORIGINAL_CHARGE_NOT_FOUND"
        mock_sqs_client.send_message.assert_not_called()

    def test_stale_processing_lock(self, mock_db, mock_sqs_client):
        db, cursor = mock_db
        message = _valid_inventory_failed_message()
        refund_ledger_id = uuid.uuid4()
        original_charge_id = uuid.uuid4()
        user_id = str(uuid.uuid4())

        stale_time = _utc_now() - timedelta(minutes=10)

        cursor.fetchone.side_effect = [
            None,  # INSERT_IDEMPOTENCY (conflict)
            ("PROCESSING", None, stale_time),  # CHECK_IDEMPOTENCY
            (str(original_charge_id), 4999, user_id, "SUCCESS"),  # GET_LEDGER_ENTRY
            (10000,),  # CREDIT_WALLET
            (refund_ledger_id,),  # INSERT_LEDGER_REFUND
        ]

        result = process_rollback(
            message=message,
            db=db,
            status_queue_url="https://sqs.us-east-1.amazonaws.com/123/status.fifo",
            sqs_client=mock_sqs_client,
        )

        assert result.success is True
        assert result.refund_ledger_id == refund_ledger_id

    def test_processing_in_progress_not_stale(self, mock_db, mock_sqs_client):
        db, cursor = mock_db
        message = _valid_inventory_failed_message()

        recent_time = _utc_now() - timedelta(minutes=2)

        cursor.fetchone.side_effect = [
            None,
            ("PROCESSING", None, recent_time),
        ]

        result = process_rollback(
            message=message,
            db=db,
            status_queue_url="https://sqs.us-east-1.amazonaws.com/123/status.fifo",
            sqs_client=mock_sqs_client,
        )

        assert result.success is False
        assert result.error == "PROCESSING_IN_PROGRESS"

    def test_previously_failed_retry(self, mock_db, mock_sqs_client):
        db, cursor = mock_db
        message = _valid_inventory_failed_message()
        refund_ledger_id = uuid.uuid4()
        original_charge_id = uuid.uuid4()
        user_id = str(uuid.uuid4())

        cursor.fetchone.side_effect = [
            None,  # INSERT_IDEMPOTENCY (conflict)
            ("FAILED", None, _utc_now()),  # CHECK_IDEMPOTENCY
            (str(original_charge_id), 4999, user_id, "SUCCESS"),  # GET_LEDGER_ENTRY
            (10000,),  # CREDIT_WALLET
            (refund_ledger_id,),  # INSERT_LEDGER_REFUND
        ]

        result = process_rollback(
            message=message,
            db=db,
            status_queue_url="https://sqs.us-east-1.amazonaws.com/123/status.fifo",
            sqs_client=mock_sqs_client,
        )

        assert result.success is True
        assert result.refund_ledger_id == refund_ledger_id

    def test_database_error_raises_saga_error(self, mock_db, mock_sqs_client):
        db, cursor = mock_db
        message = _valid_inventory_failed_message()

        cursor.fetchone.side_effect = [
            (str(message.idempotency_key),),
            psycopg2.Error("DB error"),
        ]

        with pytest.raises(SagaError):
            process_rollback(
                message=message,
                db=db,
                status_queue_url="https://sqs.us-east-1.amazonaws.com/123/status.fifo",
                sqs_client=mock_sqs_client,
            )

    def test_emits_saga_completed_event(self, mock_db, mock_sqs_client):
        db, cursor = mock_db
        message = _valid_inventory_failed_message()
        refund_ledger_id = uuid.uuid4()
        original_charge_id = uuid.uuid4()
        user_id = str(uuid.uuid4())

        cursor.fetchone.side_effect = [
            (str(message.idempotency_key),),
            (str(original_charge_id), 4999, user_id, "SUCCESS"),
            (10000,),
            (refund_ledger_id,),
            None,
        ]

        process_rollback(
            message=message,
            db=db,
            status_queue_url="https://sqs.us-east-1.amazonaws.com/123/status.fifo",
            sqs_client=mock_sqs_client,
        )

        mock_sqs_client.send_message.assert_called_once()
        call_kwargs = mock_sqs_client.send_message.call_args[1]
        assert call_kwargs["QueueUrl"] == "https://sqs.us-east-1.amazonaws.com/123/status.fifo"
        assert call_kwargs["MessageGroupId"] == str(message.transaction_id)

        body = json.loads(call_kwargs["MessageBody"])
        assert body["event_type"] == "SagaCompleted"
        assert body["outcome"] == "ROLLED_BACK"
        assert body["failure_reason"] == "SOLD_OUT"
        assert body["transaction_id"] == str(message.transaction_id)
