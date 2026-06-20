import asyncio
import logging
from datetime import UTC, datetime
from uuid import UUID

from botocore.exceptions import ClientError
from fastapi import APIRouter, Depends, HTTPException, status

from saga_initiator.dependencies import get_dynamodb_table
from saga_initiator.models.responses import SagaStatusResponse

logger = logging.getLogger(__name__)

router = APIRouter()

TIMEOUT_THRESHOLD_MINUTES = 10


@router.get(
    "/status/{transaction_id}",
    response_model=SagaStatusResponse,
)
async def get_saga_status(
    transaction_id: UUID,
    table=Depends(get_dynamodb_table),  # noqa: B008
):
    try:
        response = await asyncio.to_thread(
            lambda: table.get_item(Key={"PK": f"TXN#{transaction_id}", "SK": "RESERVATION"})
        )
    except ClientError as e:
        logger.error(
            "DynamoDB error fetching saga status: %s",
            e,
            extra={"transaction_id": str(transaction_id)},
        )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"message": "Failed to retrieve saga status"},
        ) from e

    item = response.get("Item")
    if not item:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"message": "Transaction not found"},
        )

    reservation_status = item.get("status", "UNKNOWN")
    reserved_at = item.get("reserved_at")

    if reservation_status == "RESERVED" and reserved_at:
        try:
            reserved_dt = datetime.fromisoformat(reserved_at)
            age_minutes = (datetime.now(UTC) - reserved_dt).total_seconds() / 60
            if age_minutes > TIMEOUT_THRESHOLD_MINUTES:
                reservation_status = "TIMED_OUT"
        except (ValueError, TypeError):
            pass

    return SagaStatusResponse(
        transaction_id=transaction_id,
        status=reservation_status,
        event_id=item.get("event_id"),
        tier_name=item.get("tier_name"),
        quantity=int(item["quantity"]) if "quantity" in item else None,
        reserved_at=reserved_at,
    )
