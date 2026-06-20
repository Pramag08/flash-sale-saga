import json
import logging
import uuid
from datetime import UTC, datetime

import psycopg2
from psycopg2.extensions import connection as Connection  # noqa: N812
from shared.events import PaymentSuccessEvent, ProcessPaymentEvent, SagaCompletedEvent
from shared.exceptions import SagaError
from shared.sqs_client import send_fifo_message

from payment.db import queries
from payment.models import PaymentResult

logger = logging.getLogger(__name__)

STALE_LOCK_THRESHOLD_MINUTES = 5


def process_payment(
    message: ProcessPaymentEvent,
    db: Connection,
    inventory_queue_url: str,
    status_queue_url: str,
    sqs_client=None,
) -> PaymentResult:
    transaction_id = str(message.transaction_id)
    idempotency_key = str(message.idempotency_key)

    try:
        cursor = db.cursor()

        cursor.execute(queries.INSERT_IDEMPOTENCY, (idempotency_key, transaction_id))
        result = cursor.fetchone()

        if result is None:
            cursor.execute(queries.CHECK_IDEMPOTENCY, (idempotency_key,))
            idempotency_row = cursor.fetchone()

            if idempotency_row is None:
                raise SagaError("Idempotency record not found", transaction_id)

            status, response_payload, created_at = idempotency_row

            if status == "COMPLETED":
                logger.info(
                    "Duplicate message, already processed",
                    extra={"transaction_id": transaction_id},
                )
                if response_payload:
                    cached = json.loads(response_payload)
                    return PaymentResult(
                        success=True,
                        ledger_id=uuid.UUID(cached["ledger_id"]),
                    )
                return PaymentResult(success=True)

            if status == "PROCESSING":
                age_minutes = (datetime.now(UTC) - created_at.replace(tzinfo=UTC)).total_seconds() / 60

                if age_minutes < STALE_LOCK_THRESHOLD_MINUTES:
                    logger.warning(
                        "Idempotency key in PROCESSING state",
                        extra={"transaction_id": transaction_id},
                    )
                    return PaymentResult(success=False, error="PROCESSING_IN_PROGRESS")

                logger.warning(
                    "Stale processing lock detected, reprocessing",
                    extra={"transaction_id": transaction_id},
                )
                cursor.execute(queries.DELETE_IDEMPOTENCY, (idempotency_key,))
                cursor.execute(queries.INSERT_IDEMPOTENCY, (idempotency_key, transaction_id))

            elif status == "FAILED":
                logger.warning(
                    "Previously failed, retrying",
                    extra={"transaction_id": transaction_id},
                )
                cursor.execute(queries.DELETE_IDEMPOTENCY, (idempotency_key,))
                cursor.execute(queries.INSERT_IDEMPOTENCY, (idempotency_key, transaction_id))

        db.commit()

        db.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE)
        db.autocommit = False

        try:
            cursor.execute(
                queries.DEDUCT_WALLET,
                (message.amount_cents, str(message.user_id), message.amount_cents),
            )
            wallet_result = cursor.fetchone()

            if wallet_result is None:
                db.rollback()
                cursor.execute(queries.UPDATE_IDEMPOTENCY_FAILED, (idempotency_key,))
                db.commit()
                logger.warning(
                    "Insufficient funds",
                    extra={"transaction_id": transaction_id, "user_id": str(message.user_id)},
                )

                saga_failed_event = SagaCompletedEvent(
                    transaction_id=message.transaction_id,
                    user_id=message.user_id,
                    outcome="FAILED_PERMANENTLY",
                    event_id=message.event_id,
                    tier_name=message.tier_name,
                    quantity=message.quantity,
                    amount_cents=message.amount_cents,
                    currency=message.currency,
                    completed_at=datetime.now(UTC),
                    failure_reason="INSUFFICIENT_FUNDS",
                    timestamp=datetime.now(UTC),
                )

                send_fifo_message(
                    queue_url=status_queue_url,
                    event=saga_failed_event,
                    message_group_id=transaction_id,
                    dedup_id=f"saga-completed-failed-{transaction_id}",
                    sqs_client=sqs_client,
                )

                return PaymentResult(success=False, error="INSUFFICIENT_FUNDS")

            cursor.execute(
                queries.INSERT_LEDGER_CHARGE,
                (
                    transaction_id,
                    str(message.user_id),
                    str(message.event_id),
                    message.amount_cents,
                    message.currency,
                    f"Payment for {message.tier_name} x{message.quantity}",
                ),
            )
            ledger_row = cursor.fetchone()
            ledger_id = ledger_row[0]

            cursor.execute(
                queries.UPDATE_IDEMPOTENCY_COMPLETED,
                (json.dumps({"ledger_id": str(ledger_id), "success": True}), idempotency_key),
            )

            db.commit()

        except psycopg2.Error as e:
            db.rollback()
            logger.error(
                "Database error during payment processing: %s",
                e,
                extra={"transaction_id": transaction_id},
            )
            raise

        payment_success_event = PaymentSuccessEvent(
            transaction_id=message.transaction_id,
            idempotency_key=uuid.uuid4(),
            user_id=message.user_id,
            event_id=message.event_id,
            tier_name=message.tier_name,
            quantity=message.quantity,
            amount_cents=message.amount_cents,
            currency=message.currency,
            payment_reference=ledger_id,
            charged_at=datetime.now(UTC),
            timestamp=datetime.now(UTC),
        )

        send_fifo_message(
            queue_url=inventory_queue_url,
            event=payment_success_event,
            message_group_id=transaction_id,
            dedup_id=str(payment_success_event.idempotency_key),
            sqs_client=sqs_client,
        )

        logger.info(
            "Payment charged successfully",
            extra={
                "transaction_id": transaction_id,
                "ledger_id": str(ledger_id),
                "amount_cents": message.amount_cents,
            },
        )

        return PaymentResult(success=True, ledger_id=ledger_id)

    except psycopg2.Error as e:
        logger.error(
            "Database error in process_payment: %s",
            e,
            extra={"transaction_id": transaction_id},
        )
        raise SagaError(f"Database error: {e}", transaction_id) from e
    except Exception as e:
        logger.error(
            "Unexpected error in process_payment: %s",
            e,
            extra={"transaction_id": transaction_id},
        )
        raise
