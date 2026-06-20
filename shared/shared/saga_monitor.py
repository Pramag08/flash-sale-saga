import json
import os
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

import boto3
import psycopg2
import psycopg2.extras

from shared.constants import EventType, IdempotencyStatus
from shared.events import PaymentSuccessEvent
from shared.logger import get_logger
from shared.metrics import emit_metric
from shared.sqs_client import send_fifo_message

logger = get_logger("saga-monitor")

ORPHAN_THRESHOLD_MINUTES = 10
PERMANENTLY_STUCK_THRESHOLD_MINUTES = 30

QUERY_ORPHANED_SAGAS = """
    SELECT
        idempotency_key,
        transaction_id,
        status,
        response_payload,
        created_at
    FROM idempotency_store
    WHERE status = %s
      AND created_at < %s
    ORDER BY created_at ASC
"""


def _get_db_connection():
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL environment variable is not set")
    return psycopg2.connect(database_url)


def _get_dynamodb_table():
    table_name = os.environ.get("DYNAMODB_TABLE_NAME", "FlashSaleInventory")
    dynamodb = boto3.resource("dynamodb")
    return dynamodb.Table(table_name)


def _get_sqs_client():
    return boto3.client("sqs")


def _check_reservation_exists(table, transaction_id: str) -> bool:
    try:
        response = table.get_item(
            Key={
                "PK": f"TXN#{transaction_id}",
                "SK": "RESERVATION",
            },
            ProjectionExpression="entity_type, #s",
            ExpressionAttributeNames={"#s": "status"},
        )
        return "Item" in response
    except Exception:
        return False


def _reconstruct_payment_success_event(
    response_payload: dict[str, Any],
    transaction_id: str,
) -> PaymentSuccessEvent:
    return PaymentSuccessEvent(
        event_type=EventType.PAYMENT_SUCCESS,
        version="1.0",
        transaction_id=transaction_id,
        idempotency_key=uuid4(),
        user_id=response_payload["user_id"],
        event_id=response_payload["event_id"],
        tier_name=response_payload["tier_name"],
        quantity=response_payload["quantity"],
        amount_cents=response_payload["amount_cents"],
        currency=response_payload.get("currency", "USD"),
        payment_reference=response_payload["ledger_id"],
        charged_at=response_payload["charged_at"],
        timestamp=datetime.now(UTC),
    )


def saga_timeout_handler(event: dict, context: Any) -> dict:
    now = datetime.now(UTC)
    orphan_cutoff = now - timedelta(minutes=ORPHAN_THRESHOLD_MINUTES)
    permanently_stuck_cutoff = now - timedelta(minutes=PERMANENTLY_STUCK_THRESHOLD_MINUTES)

    orphaned_recovered = 0
    permanently_stuck = 0

    conn = None
    try:
        conn = _get_db_connection()
        table = _get_dynamodb_table()
        sqs_client = _get_sqs_client()
        inventory_queue_url = os.environ["INVENTORY_QUEUE_URL"]

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(QUERY_ORPHANED_SAGAS, (IdempotencyStatus.COMPLETED, orphan_cutoff))
            rows = cur.fetchall()

        for row in rows:
            transaction_id = str(row["transaction_id"])
            created_at = row["created_at"]
            if created_at.tzinfo is None:
                created_at = created_at.replace(tzinfo=UTC)

            age_minutes = (now - created_at).total_seconds() / 60

            reservation_exists = _check_reservation_exists(table, transaction_id)
            if reservation_exists:
                continue

            response_payload = row["response_payload"]
            if isinstance(response_payload, str):
                response_payload = json.loads(response_payload)

            if response_payload is None:
                logger.error(
                    "Orphaned saga has no response_payload; cannot recover",
                    extra={"transaction_id": transaction_id},
                )
                continue

            if created_at < permanently_stuck_cutoff:
                permanently_stuck += 1
                logger.critical(
                    "Saga permanently stuck — exceeds %d minute threshold",
                    PERMANENTLY_STUCK_THRESHOLD_MINUTES,
                    extra={
                        "event": "saga_permanently_stuck",
                        "transaction_id": transaction_id,
                        "age_minutes": round(age_minutes, 1),
                    },
                )
                emit_metric("SagaPermanentlyStuck", 1.0, "Count")
                continue

            try:
                payment_success_event = _reconstruct_payment_success_event(response_payload, transaction_id)
                dedup_id = f"timeout-recovery-{uuid4()}"
                send_fifo_message(
                    queue_url=inventory_queue_url,
                    event=payment_success_event,
                    message_group_id=transaction_id,
                    dedup_id=dedup_id,
                    sqs_client=sqs_client,
                )
                orphaned_recovered += 1
                logger.warning(
                    "Re-emitted stranded PaymentSuccess for orphaned saga",
                    extra={
                        "event": "saga_timeout_recovery",
                        "transaction_id": transaction_id,
                        "age_minutes": round(age_minutes, 1),
                    },
                )
                emit_metric("SagaTimeoutRecovery", 1.0, "Count")
            except Exception:
                logger.exception(
                    "Failed to re-emit PaymentSuccess for orphaned saga",
                    extra={"transaction_id": transaction_id},
                )

    except Exception:
        logger.exception("Saga timeout monitor encountered a fatal error")
        raise
    finally:
        if conn is not None:
            conn.close()

    result = {
        "orphaned_recovered": orphaned_recovered,
        "permanently_stuck": permanently_stuck,
    }
    logger.info(
        "Saga timeout monitor completed",
        extra={
            "event": "saga_monitor_completed",
            "orphaned_recovered": orphaned_recovered,
            "permanently_stuck": permanently_stuck,
        },
    )
    return result
