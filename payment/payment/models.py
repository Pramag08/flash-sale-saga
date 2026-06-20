from dataclasses import dataclass
from uuid import UUID


@dataclass
class PaymentResult:
    success: bool
    ledger_id: UUID | None = None
    error: str | None = None


@dataclass
class RefundResult:
    success: bool
    refund_ledger_id: UUID | None = None
    error: str | None = None
