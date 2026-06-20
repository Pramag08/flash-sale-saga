#!/usr/bin/env python3
"""Local worker runner — polls SQS queues and dispatches to service handlers.

In production, AWS Lambda functions are triggered by SQS events automatically.
Locally, this script replaces the Lambda runtime by long-polling the SQS
queues and invoking the handlers directly.

It also consumes the saga-status queue to update DynamoDB with the final
saga outcome so the /status endpoint can return it.

Usage:
    ./.venv/bin/python scripts/run_workers.py
"""

import json
import logging
import signal
import threading
import time

import boto3
from botocore.exceptions import ClientError
from shared.config import get_settings
from shared.events import SagaCompletedEvent

settings = get_settings()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)s] %(levelname)s %(message)s",
)
logger = logging.getLogger("worker-runner")

sqs = boto3.client("sqs")
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(settings.DYNAMODB_TABLE_NAME)

from inventory.handler import lambda_handler as inventory_handler  # noqa: E402
from payment.handler import lambda_handler as payment_handler  # noqa: E402

running = True


class LambdaContext:
    def __init__(self):
        self.aws_request_id = "local-worker"
        self.function_name = "local-worker"


def _signal_handler(sig, frame):
    global running
    running = False
    logger.info("Shutting down workers...")


signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)


def poll_queue(queue_name: str, queue_url: str, handler):
    logger.info("Starting worker for %s", queue_name)
    while running:
        try:
            response = sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=10,
            )
            messages = response.get("Messages", [])
            if not messages:
                continue

            records = [
                {
                    "messageId": msg["MessageId"],
                    "receiptHandle": msg["ReceiptHandle"],
                    "body": msg["Body"],
                }
                for msg in messages
            ]

            event = {"Records": records}
            ctx = LambdaContext()

            result = handler(event, ctx)
            failures = {f["itemIdentifier"] for f in result.get("batchItemFailures", [])}

            for msg in messages:
                if msg["MessageId"] not in failures:
                    sqs.delete_message(
                        QueueUrl=queue_url,
                        ReceiptHandle=msg["ReceiptHandle"],
                    )

        except Exception as e:
            logger.error("Error polling %s: %s", queue_name, e, exc_info=True)
            time.sleep(5)

    logger.info("Worker for %s stopped", queue_name)


def status_consumer(queue_name: str, queue_url: str):
    """Consume SagaCompleted events and update DynamoDB so /status can return them."""
    logger.info("Starting status consumer for %s", queue_name)
    while running:
        try:
            response = sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=10,
            )
            messages = response.get("Messages", [])
            if not messages:
                continue

            for msg in messages:
                try:
                    body = json.loads(msg["Body"])
                    event = SagaCompletedEvent.model_validate(body)
                    txn_id = str(event.transaction_id)
                    pk = f"TXN#{txn_id}"

                    if event.outcome == "SUCCESS":
                        try:
                            table.update_item(
                                Key={"PK": pk, "SK": "RESERVATION"},
                                UpdateExpression="SET #s = :status",
                                ConditionExpression="#s = :reserved",
                                ExpressionAttributeNames={"#s": "status"},
                                ExpressionAttributeValues={
                                    ":status": "CONFIRMED",
                                    ":reserved": "RESERVED",
                                },
                            )
                            logger.info("Reservation confirmed: %s", txn_id)
                        except ClientError as e:
                            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                                logger.warning(
                                    "Reservation not in RESERVED state for %s, may already be confirmed",
                                    txn_id,
                                )
                            else:
                                raise

                    elif event.outcome in ("ROLLED_BACK", "FAILED_PERMANENTLY"):
                        status_value = "CANCELLED" if event.outcome == "ROLLED_BACK" else "FAILED_PERMANENTLY"
                        table.put_item(
                            Item={
                                "PK": pk,
                                "SK": "RESERVATION",
                                "entity_type": "RESERVATION",
                                "event_id": str(event.event_id),
                                "tier_name": event.tier_name,
                                "user_id": str(event.user_id),
                                "quantity": event.quantity,
                                "status": status_value,
                                "reserved_at": event.completed_at.isoformat(),
                                "failure_reason": event.failure_reason,
                            },
                        )
                        lock_pk = f"USERLOCK#{event.user_id}#{event.event_id}"
                        try:
                            table.delete_item(Key={"PK": lock_pk, "SK": "LOCK"})
                        except Exception as lock_err:
                            logger.warning(
                                "Failed to release user-event lock: %s",
                                lock_err,
                                extra={"lock_pk": lock_pk},
                            )
                        logger.info(
                            "Saga %s for %s: %s",
                            event.outcome,
                            txn_id,
                            event.failure_reason,
                        )

                    sqs.delete_message(
                        QueueUrl=queue_url,
                        ReceiptHandle=msg["ReceiptHandle"],
                    )

                except Exception as e:
                    logger.error("Error processing status message: %s", e, exc_info=True)

        except Exception as e:
            logger.error("Error polling %s: %s", queue_name, e, exc_info=True)
            time.sleep(5)

    logger.info("Status consumer for %s stopped", queue_name)


def main():
    logger.info("=== Flash Sale Saga — Local Worker Runner ===")
    logger.info("Payment queue:   %s", settings.PAYMENT_QUEUE_URL)
    logger.info("Inventory queue: %s", settings.INVENTORY_QUEUE_URL)
    logger.info("Rollback queue:  %s", settings.ROLLBACK_QUEUE_URL)
    logger.info("Status queue:    %s", settings.STATUS_QUEUE_URL)

    threads = [
        threading.Thread(
            target=poll_queue,
            args=("payment-processing", settings.PAYMENT_QUEUE_URL, payment_handler),
            name="payment-worker",
            daemon=True,
        ),
        threading.Thread(
            target=poll_queue,
            args=("payment-rollback", settings.ROLLBACK_QUEUE_URL, payment_handler),
            name="rollback-worker",
            daemon=True,
        ),
        threading.Thread(
            target=poll_queue,
            args=("inventory-processing", settings.INVENTORY_QUEUE_URL, inventory_handler),
            name="inventory-worker",
            daemon=True,
        ),
        threading.Thread(
            target=status_consumer,
            args=("saga-status", settings.STATUS_QUEUE_URL),
            name="status-worker",
            daemon=True,
        ),
    ]

    for t in threads:
        t.start()

    logger.info("All workers started. Press Ctrl+C to stop.")

    while running:
        time.sleep(1)

    for t in threads:
        t.join(timeout=15)

    logger.info("All workers stopped.")


if __name__ == "__main__":
    main()
