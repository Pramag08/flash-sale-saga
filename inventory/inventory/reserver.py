import logging
import uuid
from datetime import UTC, datetime

from botocore.exceptions import ClientError
from shared.events import InventoryFailedEvent, PaymentSuccessEvent, SagaCompletedEvent
from shared.sqs_client import send_fifo_message

from inventory.db.client import DynamoDBClient
from inventory.models import ReservationResult

logger = logging.getLogger(__name__)


def reserve_ticket(
    message: PaymentSuccessEvent,
    db: DynamoDBClient,
    status_queue_url: str,
    rollback_queue_url: str,
    sqs_client=None,
) -> ReservationResult:
    transaction_id = str(message.transaction_id)
    event_id = str(message.event_id)
    tier_name = message.tier_name
    user_id = str(message.user_id)
    quantity = message.quantity
    payment_reference = str(message.payment_reference)

    try:
        existing_reservation = db.get_reservation(transaction_id)
        if existing_reservation:
            status = existing_reservation.get("status")
            if status in ("RESERVED", "CONFIRMED"):
                logger.info(
                    "Duplicate reservation request, already processed — re-emitting SagaCompleted",
                    extra={"transaction_id": transaction_id},
                )
                _emit_saga_completed_success(
                    message=message,
                    status_queue_url=status_queue_url,
                    sqs_client=sqs_client,
                )
                return ReservationResult(success=True)

        event_metadata = db.get_event_metadata(event_id)
        if not event_metadata:
            failure_code = "EVENT_NOT_FOUND"
            failure_reason = "EVENT_NOT_FOUND"
            logger.error(
                "Event not found",
                extra={"transaction_id": transaction_id, "event_id": event_id},
            )
            _emit_inventory_failed(
                message=message,
                failure_code=failure_code,
                failure_reason=failure_reason,
                rollback_queue_url=rollback_queue_url,
                sqs_client=sqs_client,
            )
            return ReservationResult(
                success=False,
                failure_code=failure_code,
                failure_reason=failure_reason,
            )

        sale_status = event_metadata.get("sale_status")
        if sale_status != "ACTIVE":
            failure_code = "EVENT_CLOSED"
            failure_reason = "EVENT_CLOSED"
            logger.warning(
                "Event not active",
                extra={
                    "transaction_id": transaction_id,
                    "event_id": event_id,
                    "sale_status": sale_status,
                },
            )
            _emit_inventory_failed(
                message=message,
                failure_code=failure_code,
                failure_reason=failure_reason,
                rollback_queue_url=rollback_queue_url,
                sqs_client=sqs_client,
            )
            return ReservationResult(
                success=False,
                failure_code=failure_code,
                failure_reason=failure_reason,
            )

        active_reservations = db.find_active_reservation(user_id, event_id)
        if active_reservations:
            failure_code = "MAX_PER_USER_EXCEEDED"
            failure_reason = "MAX_PER_USER_EXCEEDED"
            logger.warning(
                "Active reservation already exists for user+event (backstop)",
                extra={
                    "transaction_id": transaction_id,
                    "user_id": user_id,
                    "event_id": event_id,
                    "existing_count": len(active_reservations),
                },
            )
            _emit_inventory_failed(
                message=message,
                failure_code=failure_code,
                failure_reason=failure_reason,
                rollback_queue_url=rollback_queue_url,
                sqs_client=sqs_client,
            )
            return ReservationResult(
                success=False,
                failure_code=failure_code,
                failure_reason=failure_reason,
            )

        pk = f"EVENT#{event_id}"
        sk = f"TIER#{tier_name}"
        decrement_success = db.conditional_decrement(pk, sk, quantity)

        if not decrement_success:
            failure_code = "INVENTORY_EXHAUSTED"
            failure_reason = "SOLD_OUT"
            logger.warning(
                "Inventory exhausted",
                extra={
                    "transaction_id": transaction_id,
                    "event_id": event_id,
                    "tier_name": tier_name,
                    "quantity": quantity,
                },
            )
            _emit_inventory_failed(
                message=message,
                failure_code=failure_code,
                failure_reason=failure_reason,
                rollback_queue_url=rollback_queue_url,
                sqs_client=sqs_client,
            )
            return ReservationResult(
                success=False,
                failure_code=failure_code,
                failure_reason=failure_reason,
            )

        put_success = db.put_reservation(
            transaction_id=transaction_id,
            event_id=event_id,
            tier_name=tier_name,
            user_id=user_id,
            quantity=quantity,
            payment_reference=payment_reference,
        )

        if not put_success:
            db.restore_quantity(event_id, tier_name, quantity)
            logger.info(
                "Reservation already exists (race), restored quantity",
                extra={"transaction_id": transaction_id},
            )
            _emit_saga_completed_success(
                message=message,
                status_queue_url=status_queue_url,
                sqs_client=sqs_client,
            )
            return ReservationResult(success=True)

        _emit_saga_completed_success(
            message=message,
            status_queue_url=status_queue_url,
            sqs_client=sqs_client,
        )

        logger.info(
            "Ticket reserved successfully",
            extra={
                "transaction_id": transaction_id,
                "event_id": event_id,
                "tier_name": tier_name,
                "quantity": quantity,
            },
        )

        return ReservationResult(success=True)

    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "")
        if error_code in ("ResourceNotFoundException", "ResourceNotFound"):
            logger.critical(
                "CRITICAL: DynamoDB table not found",
                extra={
                    "transaction_id": transaction_id,
                    "table_name": getattr(db, "table_name", None),
                    "error_code": error_code,
                },
            )
            try:
                _emit_inventory_failed(
                    message=message,
                    failure_code="INTERNAL_ERROR",
                    failure_reason="INTERNAL_ERROR",
                    rollback_queue_url=rollback_queue_url,
                    sqs_client=sqs_client,
                )
            except Exception as emit_err:
                logger.critical(
                    "CRITICAL: failed to emit rollback after missing DynamoDB table: %s",
                    emit_err,
                    extra={"transaction_id": transaction_id},
                )
            return ReservationResult(
                success=False,
                failure_code="INTERNAL_ERROR",
                failure_reason="INTERNAL_ERROR",
            )

        logger.error(
            "DynamoDB error in reserve_ticket: %s",
            e,
            extra={"transaction_id": transaction_id, "error_code": error_code},
        )
        raise

    except Exception as e:
        logger.error(
            "Unexpected error in reserve_ticket: %s",
            e,
            extra={"transaction_id": transaction_id},
        )
        raise


def _emit_saga_completed_success(
    message: PaymentSuccessEvent,
    status_queue_url: str,
    sqs_client=None,
) -> None:
    transaction_id = str(message.transaction_id)
    saga_completed_event = SagaCompletedEvent(
        transaction_id=message.transaction_id,
        user_id=message.user_id,
        outcome="SUCCESS",
        event_id=message.event_id,
        tier_name=message.tier_name,
        quantity=message.quantity,
        amount_cents=message.amount_cents,
        currency=message.currency,
        completed_at=datetime.now(UTC),
        failure_reason=None,
        timestamp=datetime.now(UTC),
    )

    send_fifo_message(
        queue_url=status_queue_url,
        event=saga_completed_event,
        message_group_id=transaction_id,
        dedup_id=f"saga-completed-success-{transaction_id}",
        sqs_client=sqs_client,
    )


def _emit_inventory_failed(
    message: PaymentSuccessEvent,
    failure_code: str,
    failure_reason: str,
    rollback_queue_url: str,
    sqs_client=None,
) -> None:
    inventory_failed_event = InventoryFailedEvent(
        transaction_id=message.transaction_id,
        idempotency_key=uuid.uuid4(),
        user_id=message.user_id,
        event_id=message.event_id,
        tier_name=message.tier_name,
        quantity=message.quantity,
        amount_cents=message.amount_cents,
        currency=message.currency,
        payment_reference=message.payment_reference,
        failure_reason=failure_reason,
        failure_code=failure_code,
        failed_at=datetime.now(UTC),
        timestamp=datetime.now(UTC),
    )

    send_fifo_message(
        queue_url=rollback_queue_url,
        event=inventory_failed_event,
        message_group_id=str(message.transaction_id),
        dedup_id=str(inventory_failed_event.idempotency_key),
        sqs_client=sqs_client,
    )

    logger.info(
        "InventoryFailed event emitted",
        extra={
            "transaction_id": str(message.transaction_id),
            "failure_code": failure_code,
            "failure_reason": failure_reason,
        },
    )
