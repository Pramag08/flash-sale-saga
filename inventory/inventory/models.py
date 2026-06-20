from dataclasses import dataclass


@dataclass
class ReservationResult:
    success: bool
    failure_code: str | None = None
    failure_reason: str | None = None
