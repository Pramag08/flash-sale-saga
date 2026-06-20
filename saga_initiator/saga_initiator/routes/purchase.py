import asyncio
import logging
import uuid
from datetime import UTC, datetime

from botocore.exceptions import ClientError
from fastapi import APIRouter, Depends, HTTPException, Request, status
from shared.config import get_settings
from shared.constants import RESERVATION_TTL_MINUTES
from shared.events import ProcessPaymentEvent
from shared.metrics import emit_metric
from shared.sqs_client import send_fifo_message

from saga_initiator.dependencies import get_dynamodb_table, get_sqs_client
from saga_initiator.models.requests import PurchaseRequest
from saga_initiator.models.responses import PurchaseResponse

logger = logging.getLogger(__name__)

router = APIRouter()

_settings = get_settings()


@router.post(
    "/buy-ticket",
    response_model=PurchaseResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def initiate_purchase(
    request: PurchaseRequest,
    raw_request: Request,
    sqs_client=Depends(get_sqs_client),  # noqa: B008
    table=Depends(get_dynamodb_table),  # noqa: B008
):
    transaction_id = uuid.uuid4()
    idempotency_key = uuid.uuid4()
    user_id = str(request.user_id)
    event_id = str(request.event_id)
    tier_name = request.tier_name
    quantity = request.quantity

    lock_pk = f"USERLOCK#{user_id}#{event_id}"
    lock_ttl = int(datetime.now(UTC).timestamp()) + (RESERVATION_TTL_MINUTES * 60)

    try:
        await asyncio.to_thread(
            lambda: table.put_item(
                Item={
                    "PK": lock_pk,
                    "SK": "LOCK",
                    "entity_type": "USER_LOCK",
                    "transaction_id": str(transaction_id),
                    "user_id": user_id,
                    "event_id": event_id,
                    "created_at": datetime.now(UTC).isoformat(),
                    "ttl": lock_ttl,
                },
                ConditionExpression="attribute_not_exists(PK)",
            )
        )
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "")
        if error_code == "ConditionalCheckFailedException":
            existing = await asyncio.to_thread(lambda: table.get_item(Key={"PK": lock_pk, "SK": "LOCK"}))
            existing_item = existing.get("Item", {})
            existing_txn_id = existing_item.get("transaction_id", "")
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "message": "Duplicate purchase: active reservation exists for this user and event",
                    "existing_transaction_id": existing_txn_id,
                },
            ) from None
        logger.error(
            "Failed to acquire user-event lock: %s",
            e,
            extra={
                "transaction_id": str(transaction_id),
                "user_id": user_id,
                "event_id": event_id,
                "error_code": error_code,
            },
        )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"message": "Unable to verify purchase eligibility, please retry"},
        ) from e

    try:
        tier_response = await asyncio.to_thread(
            lambda: table.get_item(Key={"PK": f"EVENT#{event_id}", "SK": f"TIER#{tier_name}"})
        )
        tier_item = tier_response.get("Item")

        if not tier_item:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={"message": "Ticket tier does not exist"},
            )

        available_qty = int(tier_item.get("available_qty", 0))
        if available_qty <= 0:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={"message": "Sold out"},
            )

        price_cents = int(tier_item["price_cents"])
        amount_cents = price_cents * quantity

        source_ip = raw_request.client.host if raw_request.client else None
        user_agent = raw_request.headers.get("user-agent")

        event = ProcessPaymentEvent(
            transaction_id=transaction_id,
            idempotency_key=idempotency_key,
            user_id=request.user_id,
            event_id=request.event_id,
            tier_name=tier_name,
            quantity=quantity,
            amount_cents=amount_cents,
            currency="USD",
            timestamp=datetime.now(UTC),
            metadata={
                "source_ip": source_ip,
                "user_agent": user_agent,
            }
            if source_ip or user_agent
            else None,
        )

        try:
            await asyncio.to_thread(
                send_fifo_message,
                queue_url=_settings.PAYMENT_QUEUE_URL,
                event=event,
                message_group_id=str(transaction_id),
                dedup_id=str(idempotency_key),
                sqs_client=sqs_client,
            )
        except Exception as e:
            logger.error(
                "Failed to send ProcessPayment event to SQS: %s",
                e,
                extra={"transaction_id": str(transaction_id)},
            )
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail={"message": "Failed to initiate saga, please retry"},
            ) from e

    except HTTPException:
        await _release_lock(table, lock_pk)
        raise
    except Exception as e:
        await _release_lock(table, lock_pk)
        logger.error(
            "Unexpected error after lock acquisition: %s",
            e,
            extra={"transaction_id": str(transaction_id)},
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"message": "Internal error"},
        ) from e

    emit_metric(
        "SagaInitiated",
        dimensions={"event_id": event_id, "tier_name": tier_name},
    )

    logger.info(
        "Saga initiated",
        extra={
            "transaction_id": str(transaction_id),
            "user_id": user_id,
            "event_id": event_id,
            "tier_name": tier_name,
            "amount_cents": amount_cents,
        },
    )

    return PurchaseResponse(
        transaction_id=transaction_id,
        status="ACCEPTED",
        message="Purchase request accepted, processing",
    )


async def _release_lock(table, lock_pk: str) -> None:
    try:
        await asyncio.to_thread(lambda: table.delete_item(Key={"PK": lock_pk, "SK": "LOCK"}))
    except Exception as e:
        logger.warning(
            "Failed to release user-event lock: %s",
            e,
            extra={"lock_pk": lock_pk},
        )
