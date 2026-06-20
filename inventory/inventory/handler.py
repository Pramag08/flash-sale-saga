import json

from shared.config import get_settings
from shared.events import PaymentSuccessEvent
from shared.exceptions import PoisonMessageError
from shared.logger import get_logger

from inventory.db.client import DynamoDBClient
from inventory.reserver import reserve_ticket

settings = None
dynamodb_client = None


def lambda_handler(event: dict, context) -> dict:
    global dynamodb_client

    s = settings if settings is not None else get_settings()
    logger = get_logger("inventory-processor", log_level=s.LOG_LEVEL)

    if dynamodb_client is None:
        dynamodb_client = DynamoDBClient(table_name=s.DYNAMODB_TABLE_NAME)

    batch_item_failures = []

    for record in event.get("Records", []):
        message_id = record.get("messageId")
        body = record.get("body")

        try:
            message_data = json.loads(body)
            message = PaymentSuccessEvent.model_validate(message_data)

            aws_request_id = getattr(context, "aws_request_id", None)
            ctx_logger = get_logger(
                "inventory-processor",
                transaction_id=str(message.transaction_id),
                aws_request_id=aws_request_id,
                log_level=s.LOG_LEVEL,
            )

            result = reserve_ticket(
                message=message,
                db=dynamodb_client,
                status_queue_url=s.STATUS_QUEUE_URL,
                rollback_queue_url=s.ROLLBACK_QUEUE_URL,
            )

            if not result.success:
                ctx_logger.warning(
                    "Ticket reservation failed",
                    extra={
                        "failure_code": result.failure_code,
                        "failure_reason": result.failure_reason,
                    },
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

    return {"batchItemFailures": batch_item_failures}
