import uuid
from datetime import UTC, datetime

import pytest
from pydantic import ValidationError
from shared.constants import (
    EventType,
    FailureCode,
    FailureReason,
    SagaOutcome,
)
from shared.events import (
    InventoryFailedEvent,
    PaymentSuccessEvent,
    ProcessPaymentEvent,
    SagaCompletedEvent,
    parse_event,
)


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _valid_process_payment_data(**overrides) -> dict:
    base = {
        "event_type": EventType.PROCESS_PAYMENT,
        "version": "1.0",
        "transaction_id": str(uuid.uuid4()),
        "idempotency_key": str(uuid.uuid4()),
        "user_id": str(uuid.uuid4()),
        "event_id": str(uuid.uuid4()),
        "tier_name": "VIP",
        "quantity": 1,
        "amount_cents": 4999,
        "currency": "USD",
        "timestamp": _utc_now().isoformat(),
    }
    base.update(overrides)
    return base


def _valid_payment_success_data(**overrides) -> dict:
    base = {
        "event_type": EventType.PAYMENT_SUCCESS,
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
        "charged_at": _utc_now().isoformat(),
        "timestamp": _utc_now().isoformat(),
    }
    base.update(overrides)
    return base


def _valid_inventory_failed_data(**overrides) -> dict:
    base = {
        "event_type": EventType.INVENTORY_FAILED,
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
        "failure_reason": FailureReason.SOLD_OUT,
        "failure_code": FailureCode.INVENTORY_EXHAUSTED,
        "failed_at": _utc_now().isoformat(),
        "timestamp": _utc_now().isoformat(),
    }
    base.update(overrides)
    return base


def _valid_saga_completed_data(**overrides) -> dict:
    base = {
        "event_type": EventType.SAGA_COMPLETED,
        "version": "1.0",
        "transaction_id": str(uuid.uuid4()),
        "user_id": str(uuid.uuid4()),
        "outcome": SagaOutcome.SUCCESS,
        "event_id": str(uuid.uuid4()),
        "tier_name": "VIP",
        "quantity": 1,
        "amount_cents": 4999,
        "currency": "USD",
        "completed_at": _utc_now().isoformat(),
        "failure_reason": None,
        "timestamp": _utc_now().isoformat(),
    }
    base.update(overrides)
    return base


class TestProcessPaymentEvent:
    def test_valid_payload(self):
        data = _valid_process_payment_data()
        event = ProcessPaymentEvent.model_validate(data)
        assert event.event_type == EventType.PROCESS_PAYMENT
        assert event.version == "1.0"
        assert event.quantity == 1
        assert event.amount_cents == 4999
        assert event.currency == "USD"
        assert event.tier_name == "VIP"

    def test_valid_with_metadata(self):
        data = _valid_process_payment_data(
            metadata={
                "source_ip": "203.0.113.42",
                "user_agent": "Mozilla/5.0",
                "flash_sale_id": "FLASH-2026-SUMMER",
            }
        )
        event = ProcessPaymentEvent.model_validate(data)
        assert event.metadata is not None
        assert event.metadata.source_ip == "203.0.113.42"
        assert event.metadata.flash_sale_id == "FLASH-2026-SUMMER"

    def test_amount_cents_zero_rejected(self):
        data = _valid_process_payment_data(amount_cents=0)
        with pytest.raises(ValidationError, match="amount_cents"):
            ProcessPaymentEvent.model_validate(data)

    def test_amount_cents_negative_rejected(self):
        data = _valid_process_payment_data(amount_cents=-100)
        with pytest.raises(ValidationError, match="amount_cents"):
            ProcessPaymentEvent.model_validate(data)

    def test_quantity_zero_rejected(self):
        data = _valid_process_payment_data(quantity=0)
        with pytest.raises(ValidationError, match="quantity"):
            ProcessPaymentEvent.model_validate(data)

    def test_quantity_negative_rejected(self):
        data = _valid_process_payment_data(quantity=-1)
        with pytest.raises(ValidationError, match="quantity"):
            ProcessPaymentEvent.model_validate(data)

    def test_invalid_currency_lowercase_rejected(self):
        data = _valid_process_payment_data(currency="usd")
        with pytest.raises(ValidationError, match="currency"):
            ProcessPaymentEvent.model_validate(data)

    def test_invalid_currency_wrong_length_rejected(self):
        data = _valid_process_payment_data(currency="US")
        with pytest.raises(ValidationError, match="currency"):
            ProcessPaymentEvent.model_validate(data)

    def test_invalid_uuid_rejected(self):
        data = _valid_process_payment_data(transaction_id="not-a-uuid")
        with pytest.raises(ValidationError):
            ProcessPaymentEvent.model_validate(data)

    def test_timestamp_without_timezone_rejected(self):
        data = _valid_process_payment_data(timestamp="2026-06-20T00:00:00")
        with pytest.raises(ValidationError, match="timezone"):
            ProcessPaymentEvent.model_validate(data)

    def test_empty_tier_name_rejected(self):
        data = _valid_process_payment_data(tier_name="")
        with pytest.raises(ValidationError, match="tier_name"):
            ProcessPaymentEvent.model_validate(data)

    def test_missing_required_field_rejected(self):
        data = _valid_process_payment_data()
        del data["user_id"]
        with pytest.raises(ValidationError):
            ProcessPaymentEvent.model_validate(data)

    def test_invalid_version_rejected(self):
        data = _valid_process_payment_data(version="abc")
        with pytest.raises(ValidationError, match="version"):
            ProcessPaymentEvent.model_validate(data)

    def test_serialization_roundtrip(self):
        data = _valid_process_payment_data()
        event = ProcessPaymentEvent.model_validate(data)
        json_str = event.model_dump_json()
        restored = ProcessPaymentEvent.model_validate_json(json_str)
        assert restored.transaction_id == event.transaction_id
        assert restored.amount_cents == event.amount_cents


class TestPaymentSuccessEvent:
    def test_valid_payload(self):
        data = _valid_payment_success_data()
        event = PaymentSuccessEvent.model_validate(data)
        assert event.event_type == EventType.PAYMENT_SUCCESS
        assert event.payment_reference is not None
        assert event.charged_at is not None

    def test_amount_cents_zero_rejected(self):
        data = _valid_payment_success_data(amount_cents=0)
        with pytest.raises(ValidationError, match="amount_cents"):
            PaymentSuccessEvent.model_validate(data)

    def test_quantity_zero_rejected(self):
        data = _valid_payment_success_data(quantity=0)
        with pytest.raises(ValidationError, match="quantity"):
            PaymentSuccessEvent.model_validate(data)

    def test_missing_payment_reference_rejected(self):
        data = _valid_payment_success_data()
        del data["payment_reference"]
        with pytest.raises(ValidationError):
            PaymentSuccessEvent.model_validate(data)

    def test_missing_charged_at_rejected(self):
        data = _valid_payment_success_data()
        del data["charged_at"]
        with pytest.raises(ValidationError):
            PaymentSuccessEvent.model_validate(data)

    def test_invalid_currency_rejected(self):
        data = _valid_payment_success_data(currency="EUR1")
        with pytest.raises(ValidationError, match="currency"):
            PaymentSuccessEvent.model_validate(data)


class TestInventoryFailedEvent:
    def test_valid_payload(self):
        data = _valid_inventory_failed_data()
        event = InventoryFailedEvent.model_validate(data)
        assert event.event_type == EventType.INVENTORY_FAILED
        assert event.failure_reason == FailureReason.SOLD_OUT
        assert event.failure_code == FailureCode.INVENTORY_EXHAUSTED

    def test_all_failure_reasons(self):
        for reason in FailureReason:
            data = _valid_inventory_failed_data(failure_reason=reason)
            event = InventoryFailedEvent.model_validate(data)
            assert event.failure_reason == reason

    def test_all_failure_codes(self):
        for code in FailureCode:
            data = _valid_inventory_failed_data(failure_code=code)
            event = InventoryFailedEvent.model_validate(data)
            assert event.failure_code == code

    def test_invalid_failure_reason_rejected(self):
        data = _valid_inventory_failed_data(failure_reason="INVALID_REASON")
        with pytest.raises(ValidationError):
            InventoryFailedEvent.model_validate(data)

    def test_invalid_failure_code_rejected(self):
        data = _valid_inventory_failed_data(failure_code="INVALID_CODE")
        with pytest.raises(ValidationError):
            InventoryFailedEvent.model_validate(data)

    def test_amount_cents_zero_rejected(self):
        data = _valid_inventory_failed_data(amount_cents=0)
        with pytest.raises(ValidationError, match="amount_cents"):
            InventoryFailedEvent.model_validate(data)

    def test_missing_failed_at_rejected(self):
        data = _valid_inventory_failed_data()
        del data["failed_at"]
        with pytest.raises(ValidationError):
            InventoryFailedEvent.model_validate(data)


class TestSagaCompletedEvent:
    def test_valid_success(self):
        data = _valid_saga_completed_data()
        event = SagaCompletedEvent.model_validate(data)
        assert event.event_type == EventType.SAGA_COMPLETED
        assert event.outcome == SagaOutcome.SUCCESS
        assert event.failure_reason is None

    def test_valid_rolled_back(self):
        data = _valid_saga_completed_data(
            outcome=SagaOutcome.ROLLED_BACK,
            failure_reason="SOLD_OUT",
        )
        event = SagaCompletedEvent.model_validate(data)
        assert event.outcome == SagaOutcome.ROLLED_BACK
        assert event.failure_reason == "SOLD_OUT"

    def test_valid_failed_permanently(self):
        data = _valid_saga_completed_data(
            outcome=SagaOutcome.FAILED_PERMANENTLY,
            failure_reason="INSUFFICIENT_FUNDS",
        )
        event = SagaCompletedEvent.model_validate(data)
        assert event.outcome == SagaOutcome.FAILED_PERMANENTLY

    def test_failure_reason_required_when_not_success(self):
        data = _valid_saga_completed_data(
            outcome=SagaOutcome.ROLLED_BACK,
            failure_reason=None,
        )
        with pytest.raises(ValidationError, match="failure_reason"):
            SagaCompletedEvent.model_validate(data)

    def test_failure_reason_must_be_null_on_success(self):
        data = _valid_saga_completed_data(
            outcome=SagaOutcome.SUCCESS,
            failure_reason="SOLD_OUT",
        )
        with pytest.raises(ValidationError, match="failure_reason"):
            SagaCompletedEvent.model_validate(data)

    def test_amount_cents_zero_rejected(self):
        data = _valid_saga_completed_data(amount_cents=0)
        with pytest.raises(ValidationError, match="amount_cents"):
            SagaCompletedEvent.model_validate(data)

    def test_quantity_zero_rejected(self):
        data = _valid_saga_completed_data(quantity=0)
        with pytest.raises(ValidationError, match="quantity"):
            SagaCompletedEvent.model_validate(data)

    def test_invalid_outcome_rejected(self):
        data = _valid_saga_completed_data(outcome="UNKNOWN")
        with pytest.raises(ValidationError):
            SagaCompletedEvent.model_validate(data)

    def test_timestamp_without_timezone_rejected(self):
        data = _valid_saga_completed_data(
            timestamp="2026-06-20T00:00:00",
            completed_at=_utc_now().isoformat(),
        )
        with pytest.raises(ValidationError, match="timezone"):
            SagaCompletedEvent.model_validate(data)

    def test_completed_at_without_timezone_rejected(self):
        data = _valid_saga_completed_data(
            completed_at="2026-06-20T00:00:00",
        )
        with pytest.raises(ValidationError, match="timezone"):
            SagaCompletedEvent.model_validate(data)


class TestParseEvent:
    def test_parse_process_payment(self):
        data = _valid_process_payment_data()
        event = parse_event(data)
        assert isinstance(event, ProcessPaymentEvent)

    def test_parse_payment_success(self):
        data = _valid_payment_success_data()
        event = parse_event(data)
        assert isinstance(event, PaymentSuccessEvent)

    def test_parse_inventory_failed(self):
        data = _valid_inventory_failed_data()
        event = parse_event(data)
        assert isinstance(event, InventoryFailedEvent)

    def test_parse_saga_completed(self):
        data = _valid_saga_completed_data()
        event = parse_event(data)
        assert isinstance(event, SagaCompletedEvent)

    def test_missing_event_type_raises(self):
        data = _valid_process_payment_data()
        del data["event_type"]
        with pytest.raises(ValueError, match="Missing 'event_type'"):
            parse_event(data)

    def test_unknown_event_type_raises(self):
        data = _valid_process_payment_data(event_type="UnknownEvent")
        with pytest.raises(ValueError, match="Unknown event_type"):
            parse_event(data)
