from shared.logger import get_logger

from inventory.cleanup import ttl_cleanup_handler
from inventory.db.client import DynamoDBClient

logger = get_logger("reservation-cleanup", log_level="INFO")


def lambda_handler(event: dict, context) -> None:
    table_name = "FlashSaleInventory"
    db = DynamoDBClient(table_name=table_name)

    try:
        ttl_cleanup_handler(event=event, context=context, db=db)
    except Exception as e:
        logger.error(
            "Error in TTL cleanup handler",
            extra={"error": str(e)},
        )
        raise
