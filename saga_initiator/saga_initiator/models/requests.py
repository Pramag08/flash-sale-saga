from uuid import UUID

from pydantic import BaseModel, Field


class PurchaseRequest(BaseModel):
    user_id: UUID
    event_id: UUID
    tier_name: str = Field(min_length=1)
    quantity: int = Field(gt=0)
