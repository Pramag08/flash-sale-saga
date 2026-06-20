import logging
from datetime import UTC, datetime
from typing import Any

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class DynamoDBClient:
    def __init__(self, table_name: str, dynamodb_resource=None):
        self.table_name = table_name
        self.dynamodb = dynamodb_resource or boto3.resource("dynamodb")
        self.table = self.dynamodb.Table(table_name)

    def conditional_decrement(self, pk: str, sk: str, qty: int) -> bool:
        try:
            now = datetime.now(UTC).isoformat()
            self.table.update_item(
                Key={"PK": pk, "SK": sk},
                UpdateExpression="SET available_qty = available_qty - :qty, updated_at = :now",
                ConditionExpression="available_qty >= :qty",
                ExpressionAttributeValues={
                    ":qty": qty,
                    ":now": now,
                },
            )
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                logger.warning(
                    "Conditional check failed: insufficient quantity",
                    extra={"pk": pk, "sk": sk, "qty": qty},
                )
                return False
            raise

    def put_reservation(
        self,
        transaction_id: str,
        event_id: str,
        tier_name: str,
        user_id: str,
        quantity: int,
        payment_reference: str,
        ttl_minutes: int = 15,
    ) -> bool:
        now = datetime.now(UTC)
        ttl_epoch = int(now.timestamp()) + (ttl_minutes * 60)

        item = {
            "PK": f"TXN#{transaction_id}",
            "SK": "RESERVATION",
            "GSI1PK": f"USER#{user_id}",
            "GSI1SK": f"TXN#{transaction_id}",
            "entity_type": "RESERVATION",
            "event_id": event_id,
            "tier_name": tier_name,
            "user_id": user_id,
            "quantity": quantity,
            "status": "RESERVED",
            "payment_reference": payment_reference,
            "reserved_at": now.isoformat(),
            "ttl": ttl_epoch,
        }

        try:
            self.table.put_item(
                Item=item,
                ConditionExpression="attribute_not_exists(PK)",
            )
            logger.info(
                "Reservation created",
                extra={
                    "transaction_id": transaction_id,
                    "event_id": event_id,
                    "tier_name": tier_name,
                    "quantity": quantity,
                },
            )
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                logger.warning(
                    "Reservation already exists for transaction",
                    extra={"transaction_id": transaction_id},
                )
                return False
            raise

    def find_active_reservation(self, user_id: str, event_id: str) -> list[dict[str, Any]]:
        response = self.table.query(
            IndexName="GSI1",
            KeyConditionExpression="GSI1PK = :user_key",
            FilterExpression="event_id = :event_id AND #s IN (:reserved, :confirmed)",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={
                ":user_key": f"USER#{user_id}",
                ":event_id": event_id,
                ":reserved": "RESERVED",
                ":confirmed": "CONFIRMED",
            },
        )
        return response.get("Items", [])

    def get_reservation(self, transaction_id: str) -> dict[str, Any] | None:
        response = self.table.get_item(Key={"PK": f"TXN#{transaction_id}", "SK": "RESERVATION"})
        return response.get("Item")

    def get_event_metadata(self, event_id: str) -> dict[str, Any] | None:
        response = self.table.get_item(Key={"PK": f"EVENT#{event_id}", "SK": "METADATA"})
        return response.get("Item")

    def get_ticket_tier(self, event_id: str, tier_name: str) -> dict[str, Any] | None:
        response = self.table.get_item(Key={"PK": f"EVENT#{event_id}", "SK": f"TIER#{tier_name}"})
        return response.get("Item")

    def restore_quantity(self, event_id: str, tier_name: str, quantity: int) -> None:
        now = datetime.now(UTC).isoformat()
        self.table.update_item(
            Key={"PK": f"EVENT#{event_id}", "SK": f"TIER#{tier_name}"},
            UpdateExpression="SET available_qty = available_qty + :qty, updated_at = :now",
            ExpressionAttributeValues={
                ":qty": quantity,
                ":now": now,
            },
        )
        logger.info(
            "Quantity restored",
            extra={
                "event_id": event_id,
                "tier_name": tier_name,
                "quantity": quantity,
            },
        )
