from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field, field_validator, model_validator

from shared.constants import EventType, FailureCode, FailureReason, SagaOutcome


class BaseEvent(BaseModel):
    event_type: str
    version: str = Field(default="1.0", pattern=r"^\d+\.\d+$")
    transaction_id: UUID
    timestamp: datetime

    @field_validator("timestamp")
    @classmethod
    def timestamp_must_be_utc(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("timestamp must include timezone info")
        return v


class EventMetadata(BaseModel):
    source_ip: str | None = None
    user_agent: str | None = None
    flash_sale_id: str | None = None


class ProcessPaymentEvent(BaseEvent):
    event_type: str = Field(default=EventType.PROCESS_PAYMENT, frozen=True)
    idempotency_key: UUID
    user_id: UUID
    event_id: UUID
    tier_name: str = Field(min_length=1)
    quantity: int = Field(gt=0)
    amount_cents: int = Field(gt=0)
    currency: str = Field(pattern=r"^[A-Z]{3}$")
    metadata: EventMetadata | None = None


class PaymentSuccessEvent(BaseEvent):
    event_type: str = Field(default=EventType.PAYMENT_SUCCESS, frozen=True)
    idempotency_key: UUID
    user_id: UUID
    event_id: UUID
    tier_name: str = Field(min_length=1)
    quantity: int = Field(gt=0)
    amount_cents: int = Field(gt=0)
    currency: str = Field(pattern=r"^[A-Z]{3}$")
    payment_reference: UUID
    charged_at: datetime


class InventoryFailedEvent(BaseEvent):
    event_type: str = Field(default=EventType.INVENTORY_FAILED, frozen=True)
    idempotency_key: UUID
    user_id: UUID
    event_id: UUID
    tier_name: str = Field(min_length=1)
    quantity: int = Field(gt=0)
    amount_cents: int = Field(gt=0)
    currency: str = Field(pattern=r"^[A-Z]{3}$")
    payment_reference: UUID
    failure_reason: FailureReason
    failure_code: FailureCode
    failed_at: datetime


class SagaCompletedEvent(BaseModel):
    event_type: str = Field(default=EventType.SAGA_COMPLETED, frozen=True)
    version: str = Field(default="1.0", pattern=r"^\d+\.\d+$")
    transaction_id: UUID
    user_id: UUID
    outcome: SagaOutcome
    event_id: UUID
    tier_name: str = Field(min_length=1)
    quantity: int = Field(gt=0)
    amount_cents: int = Field(gt=0)
    currency: str = Field(pattern=r"^[A-Z]{3}$")
    completed_at: datetime
    failure_reason: str | None = None
    timestamp: datetime

    @field_validator("timestamp", "completed_at")
    @classmethod
    def timestamps_must_be_utc(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("timestamp must include timezone info")
        return v

    @model_validator(mode="after")
    def failure_reason_consistency(self) -> "SagaCompletedEvent":
        if self.outcome != SagaOutcome.SUCCESS and self.failure_reason is None:
            raise ValueError("failure_reason must be provided when outcome is not SUCCESS")
        if self.outcome == SagaOutcome.SUCCESS and self.failure_reason is not None:
            raise ValueError("failure_reason must be null when outcome is SUCCESS")
        return self


EVENT_MODELS: dict[str, type[BaseModel]] = {
    EventType.PROCESS_PAYMENT: ProcessPaymentEvent,
    EventType.PAYMENT_SUCCESS: PaymentSuccessEvent,
    EventType.INVENTORY_FAILED: InventoryFailedEvent,
    EventType.SAGA_COMPLETED: SagaCompletedEvent,
}


def parse_event(data: dict[str, Any]) -> BaseModel:
    event_type = data.get("event_type")
    if event_type is None:
        raise ValueError("Missing 'event_type' field in event payload")
    model_cls = EVENT_MODELS.get(event_type)
    if model_cls is None:
        raise ValueError(f"Unknown event_type: {event_type}")
    return model_cls.model_validate(data)
