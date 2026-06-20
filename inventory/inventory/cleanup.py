import logging

from inventory.db.client import DynamoDBClient

logger = logging.getLogger(__name__)


def ttl_cleanup_handler(event: dict, context, db: DynamoDBClient) -> None:
    for record in event.get("Records", []):
        if record.get("eventName") != "REMOVE":
            continue

        user_identity = record.get("userIdentity", {})
        if user_identity.get("type") != "Service":
            continue

        if user_identity.get("principalId") != "dynamodb.amazonaws.com":
            continue

        old_image = record.get("dynamodb", {}).get("OldImage", {})
        if not old_image:
            continue

        entity_type = old_image.get("entity_type", {}).get("S")
        if entity_type != "RESERVATION":
            continue

        event_id = old_image.get("event_id", {}).get("S")
        tier_name = old_image.get("tier_name", {}).get("S")
        quantity_str = old_image.get("quantity", {}).get("N")

        if not all([event_id, tier_name, quantity_str]):
            logger.warning(
                "Missing required fields in expired reservation",
                extra={"old_image": old_image},
            )
            continue

        quantity = int(quantity_str)

        try:
            db.restore_quantity(event_id, tier_name, quantity)
            logger.info(
                "Reservation expired, quantity restored",
                extra={
                    "event_id": event_id,
                    "tier_name": tier_name,
                    "quantity": quantity,
                },
            )
        except Exception as e:
            logger.error(
                "Failed to restore quantity for expired reservation: %s",
                e,
                extra={
                    "event_id": event_id,
                    "tier_name": tier_name,
                    "quantity": quantity,
                },
            )
            raise
