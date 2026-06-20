import logging
import time
from typing import TYPE_CHECKING

import boto3
from botocore.exceptions import ClientError

if TYPE_CHECKING:
    from pydantic import BaseModel

logger = logging.getLogger(__name__)

RETRYABLE_ERRORS = {"ServiceUnavailable", "ThrottlingException", "RequestThrottled"}
MAX_RETRIES = 3
BASE_BACKOFF_MS = 100


def send_fifo_message(
    queue_url: str,
    event: "BaseModel",
    message_group_id: str,
    dedup_id: str,
    sqs_client=None,
) -> str:
    body = event.model_dump_json()

    client = sqs_client or boto3.client("sqs")

    for attempt in range(MAX_RETRIES):
        try:
            response = client.send_message(
                QueueUrl=queue_url,
                MessageBody=body,
                MessageGroupId=message_group_id,
                MessageDeduplicationId=dedup_id,
            )
            message_id = response["MessageId"]
            logger.info(
                "SQS message sent",
                extra={
                    "queue": queue_url,
                    "message_id": message_id,
                    "message_group_id": message_group_id,
                },
            )
            return message_id
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code in RETRYABLE_ERRORS and attempt < MAX_RETRIES - 1:
                backoff_ms = BASE_BACKOFF_MS * (2**attempt)
                time.sleep(backoff_ms / 1000)
                continue
            raise

    raise RuntimeError("Max retries exceeded")
