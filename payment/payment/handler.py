import json

from shared.config import get_settings
from shared.events import EVENT_MODELS, InventoryFailedEvent, ProcessPaymentEvent
from shared.exceptions import PoisonMessageError
from shared.logger import get_logger

from payment.db.connection import get_connection, release_connection
from payment.processor import process_payment
from payment.rollback import process_rollback

settings = None


def lambda_handler(event: dict, context) -> dict:
    s = settings if settings is not None else get_settings()
    logger = get_logger("payment-processor", log_level=s.LOG_LEVEL)
    batch_item_failures = []

    db = get_connection(s.DATABASE_URL)

    try:
        for record in event.get("Records", []):
            message_id = record.get("messageId")
            body = record.get("body")

            try:
                message_data = json.loads(body)
                event_type = message_data.get("event_type")
                model_cls = EVENT_MODELS.get(event_type)

                if model_cls is None:
                    logger.critical(
                        "Poison message: unknown event_type",
                        extra={"body": body, "event_type": event_type},
                    )
                    continue

                message = model_cls.model_validate(message_data)

                aws_request_id = getattr(context, "aws_request_id", None)
                ctx_logger = get_logger(
                    "payment-processor",
                    transaction_id=str(message.transaction_id),
                    aws_request_id=aws_request_id,
                    log_level=s.LOG_LEVEL,
                )

                if isinstance(message, ProcessPaymentEvent):
                    result = process_payment(
                        message=message,
                        db=db,
                        inventory_queue_url=s.INVENTORY_QUEUE_URL,
                        status_queue_url=s.STATUS_QUEUE_URL,
                        sqs_client=None,
                    )
                    if not result.success:
                        ctx_logger.warning(
                            "Payment processing failed",
                            extra={"error": result.error},
                        )

                elif isinstance(message, InventoryFailedEvent):
                    result = process_rollback(
                        message=message,
                        db=db,
                        status_queue_url=s.STATUS_QUEUE_URL,
                        sqs_client=None,
                    )
                    if not result.success:
                        ctx_logger.warning(
                            "Rollback processing failed",
                            extra={"error": result.error},
                        )

                else:
                    logger.error(
                        "Unhandled event type for payment service",
                        extra={"event_type": event_type, "message_id": message_id},
                    )

            except json.JSONDecodeError as e:
                logger.critical(
                    "Poison message: invalid JSON",
                    extra={"body": body, "error": str(e)},
                )
                continue

            except PoisonMessageError as e:
                logger.critical(
                    "Poison message: invalid schema",
                    extra={"error": str(e)},
                )
                continue

            except Exception as e:
                logger.error(
                    "Error processing message",
                    extra={"error": str(e), "message_id": message_id},
                )
                batch_item_failures.append({"itemIdentifier": message_id})

    finally:
        release_connection(db, s.DATABASE_URL)

    return {"batchItemFailures": batch_item_failures}
