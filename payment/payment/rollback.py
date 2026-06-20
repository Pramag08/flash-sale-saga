import json
import logging
import uuid
from datetime import UTC, datetime

import psycopg2
from psycopg2.extensions import connection as Connection  # noqa: N812
from shared.events import InventoryFailedEvent, SagaCompletedEvent
from shared.exceptions import SagaError
from shared.sqs_client import send_fifo_message

from payment.db import queries
from payment.models import RefundResult

logger = logging.getLogger(__name__)


def process_rollback(
    message: InventoryFailedEvent,
    db: Connection,
    status_queue_url: str,
    sqs_client=None,
) -> RefundResult:
    transaction_id = str(message.transaction_id)
    idempotency_key = str(message.idempotency_key)
    payment_reference = str(message.payment_reference)

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
                    "Duplicate rollback message, already processed",
                    extra={"transaction_id": transaction_id},
                )
                if response_payload:
                    cached = json.loads(response_payload)
                    return RefundResult(
                        success=True,
                        refund_ledger_id=uuid.UUID(cached["refund_ledger_id"]),
                    )
                return RefundResult(success=True)

            if status == "PROCESSING":
                age_minutes = (datetime.now(UTC) - created_at.replace(tzinfo=UTC)).total_seconds() / 60

                if age_minutes < 5:
                    logger.warning(
                        "Rollback idempotency key in PROCESSING state",
                        extra={"transaction_id": transaction_id},
                    )
                    return RefundResult(success=False, error="PROCESSING_IN_PROGRESS")

                logger.warning(
                    "Stale rollback lock detected, reprocessing",
                    extra={"transaction_id": transaction_id},
                )
                cursor.execute(queries.DELETE_IDEMPOTENCY, (idempotency_key,))
                cursor.execute(queries.INSERT_IDEMPOTENCY, (idempotency_key, transaction_id))

            elif status == "FAILED":
                logger.warning(
                    "Previously failed rollback, retrying",
                    extra={"transaction_id": transaction_id},
                )
                cursor.execute(queries.DELETE_IDEMPOTENCY, (idempotency_key,))
                cursor.execute(queries.INSERT_IDEMPOTENCY, (idempotency_key, transaction_id))

        db.commit()

        cursor.execute(queries.GET_LEDGER_ENTRY, (payment_reference,))
        charge_row = cursor.fetchone()

        if charge_row is None:
            logger.critical(
                "Refund requested for non-existent charge",
                extra={"transaction_id": transaction_id, "payment_reference": payment_reference},
            )
            cursor.execute(queries.UPDATE_IDEMPOTENCY_FAILED, (idempotency_key,))
            db.commit()
            return RefundResult(success=False, error="ORIGINAL_CHARGE_NOT_FOUND")

        _, amount_cents, user_id, _ = charge_row

        db.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE)
        db.autocommit = False

        try:
            cursor.execute(queries.CREDIT_WALLET, (amount_cents, user_id))
            wallet_result = cursor.fetchone()

            if wallet_result is None:
                db.rollback()
                raise SagaError("Failed to credit wallet", transaction_id)

            cursor.execute(
                queries.INSERT_LEDGER_REFUND,
                (
                    transaction_id,
                    user_id,
                    str(message.event_id),
                    amount_cents,
                    message.currency,
                    f"Compensating refund: {message.failure_reason}",
                ),
            )
            refund_row = cursor.fetchone()
            refund_ledger_id = refund_row[0]

            cursor.execute(
                queries.UPDATE_IDEMPOTENCY_COMPLETED,
                (json.dumps({"refund_ledger_id": str(refund_ledger_id), "success": True}), idempotency_key),
            )

            db.commit()

        except psycopg2.Error as e:
            db.rollback()
            logger.error(
                "Database error during refund processing: %s",
                e,
                extra={"transaction_id": transaction_id},
            )
            raise

        saga_completed_event = SagaCompletedEvent(
            transaction_id=message.transaction_id,
            user_id=message.user_id,
            outcome="ROLLED_BACK",
            event_id=message.event_id,
            tier_name=message.tier_name,
            quantity=message.quantity,
            amount_cents=message.amount_cents,
            currency=message.currency,
            completed_at=datetime.now(UTC),
            failure_reason=message.failure_reason,
            timestamp=datetime.now(UTC),
        )

        send_fifo_message(
            queue_url=status_queue_url,
            event=saga_completed_event,
            message_group_id=transaction_id,
            dedup_id=f"saga-completed-rolledback-{transaction_id}",
            sqs_client=sqs_client,
        )

        logger.info(
            "Refund executed successfully",
            extra={
                "transaction_id": transaction_id,
                "refund_ledger_id": str(refund_ledger_id),
                "amount_cents": amount_cents,
            },
        )

        return RefundResult(success=True, refund_ledger_id=refund_ledger_id)

    except psycopg2.Error as e:
        logger.error(
            "Database error in process_rollback: %s",
            e,
            extra={"transaction_id": transaction_id},
        )
        raise SagaError(f"Database error: {e}", transaction_id) from e
    except Exception as e:
        logger.error(
            "Unexpected error in process_rollback: %s",
            e,
            extra={"transaction_id": transaction_id},
        )
        raise
