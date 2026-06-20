from dotenv import load_dotenv
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

load_dotenv()


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="ignore",
    )

    AWS_REGION: str = Field(default="us-east-1", description="AWS region for all services")

    PAYMENT_QUEUE_URL: str = Field(description="SQS FIFO queue URL for payment processing")
    INVENTORY_QUEUE_URL: str = Field(description="SQS FIFO queue URL for inventory processing")
    ROLLBACK_QUEUE_URL: str = Field(description="SQS FIFO queue URL for payment rollback")
    STATUS_QUEUE_URL: str = Field(description="SQS FIFO queue URL for saga status notifications")

    DYNAMODB_TABLE_NAME: str = Field(
        default="FlashSaleInventory",
        description="DynamoDB table name for inventory",
    )

    DATABASE_URL: str = Field(description="PostgreSQL connection string for payment service")

    LOG_LEVEL: str = Field(
        default="INFO",
        description="Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)",
    )


def get_settings() -> Settings:
    return Settings()  # type: ignore[call-arg]
