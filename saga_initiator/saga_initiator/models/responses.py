from uuid import UUID

from pydantic import BaseModel


class PurchaseResponse(BaseModel):
    transaction_id: UUID
    status: str
    message: str


class SagaStatusResponse(BaseModel):
    transaction_id: UUID
    status: str
    event_id: str | None = None
    tier_name: str | None = None
    quantity: int | None = None
    reserved_at: str | None = None
