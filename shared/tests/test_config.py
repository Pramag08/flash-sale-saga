import pytest
from pydantic import ValidationError
from shared.config import Settings, get_settings


class TestSettings:
    def test_valid_settings_from_env(self, monkeypatch):
        monkeypatch.setenv("AWS_REGION", "us-west-2")
        monkeypatch.setenv("PAYMENT_QUEUE_URL", "https://sqs.us-west-2.amazonaws.com/123/payment.fifo")
        monkeypatch.setenv("INVENTORY_QUEUE_URL", "https://sqs.us-west-2.amazonaws.com/123/inventory.fifo")
        monkeypatch.setenv("ROLLBACK_QUEUE_URL", "https://sqs.us-west-2.amazonaws.com/123/rollback.fifo")
        monkeypatch.setenv("STATUS_QUEUE_URL", "https://sqs.us-west-2.amazonaws.com/123/status.fifo")
        monkeypatch.setenv("DYNAMODB_TABLE_NAME", "TestTable")
        monkeypatch.setenv("DATABASE_URL", "postgresql://user:pass@localhost:5432/testdb")
        monkeypatch.setenv("LOG_LEVEL", "DEBUG")

        settings = Settings()

        assert settings.AWS_REGION == "us-west-2"
        assert settings.PAYMENT_QUEUE_URL == "https://sqs.us-west-2.amazonaws.com/123/payment.fifo"
        assert settings.INVENTORY_QUEUE_URL == "https://sqs.us-west-2.amazonaws.com/123/inventory.fifo"
        assert settings.ROLLBACK_QUEUE_URL == "https://sqs.us-west-2.amazonaws.com/123/rollback.fifo"
        assert settings.STATUS_QUEUE_URL == "https://sqs.us-west-2.amazonaws.com/123/status.fifo"
        assert settings.DYNAMODB_TABLE_NAME == "TestTable"
        assert settings.DATABASE_URL == "postgresql://user:pass@localhost:5432/testdb"
        assert settings.LOG_LEVEL == "DEBUG"

    def test_default_values(self, monkeypatch):
        monkeypatch.setenv("PAYMENT_QUEUE_URL", "https://sqs.us-east-1.amazonaws.com/123/payment.fifo")
        monkeypatch.setenv("INVENTORY_QUEUE_URL", "https://sqs.us-east-1.amazonaws.com/123/inventory.fifo")
        monkeypatch.setenv("ROLLBACK_QUEUE_URL", "https://sqs.us-east-1.amazonaws.com/123/rollback.fifo")
        monkeypatch.setenv("STATUS_QUEUE_URL", "https://sqs.us-east-1.amazonaws.com/123/status.fifo")
        monkeypatch.setenv("DATABASE_URL", "postgresql://user:pass@localhost:5432/testdb")

        settings = Settings()

        assert settings.AWS_REGION == "us-east-1"
        assert settings.DYNAMODB_TABLE_NAME == "FlashSaleInventory"
        assert settings.LOG_LEVEL == "INFO"

    def test_missing_required_field_raises(self, monkeypatch):
        monkeypatch.setenv("AWS_REGION", "us-east-1")
        monkeypatch.setenv("PAYMENT_QUEUE_URL", "https://sqs.us-east-1.amazonaws.com/123/payment.fifo")
        monkeypatch.setenv("INVENTORY_QUEUE_URL", "https://sqs.us-east-1.amazonaws.com/123/inventory.fifo")
        monkeypatch.setenv("ROLLBACK_QUEUE_URL", "https://sqs.us-east-1.amazonaws.com/123/rollback.fifo")
        monkeypatch.setenv("STATUS_QUEUE_URL", "https://sqs.us-east-1.amazonaws.com/123/status.fifo")
        monkeypatch.setenv("DYNAMODB_TABLE_NAME", "TestTable")
        monkeypatch.delenv("DATABASE_URL", raising=False)

        with pytest.raises(ValidationError):
            Settings(_env_file=None)

    def test_extra_env_vars_ignored(self, monkeypatch):
        monkeypatch.setenv("PAYMENT_QUEUE_URL", "https://sqs.us-east-1.amazonaws.com/123/payment.fifo")
        monkeypatch.setenv("INVENTORY_QUEUE_URL", "https://sqs.us-east-1.amazonaws.com/123/inventory.fifo")
        monkeypatch.setenv("ROLLBACK_QUEUE_URL", "https://sqs.us-east-1.amazonaws.com/123/rollback.fifo")
        monkeypatch.setenv("STATUS_QUEUE_URL", "https://sqs.us-east-1.amazonaws.com/123/status.fifo")
        monkeypatch.setenv("DATABASE_URL", "postgresql://user:pass@localhost:5432/testdb")
        monkeypatch.setenv("UNKNOWN_VAR", "should_be_ignored")

        settings = Settings()
        assert not hasattr(settings, "UNKNOWN_VAR")


class TestGetSettings:
    def test_get_settings_returns_settings_instance(self, monkeypatch):
        monkeypatch.setenv("PAYMENT_QUEUE_URL", "https://sqs.us-east-1.amazonaws.com/123/payment.fifo")
        monkeypatch.setenv("INVENTORY_QUEUE_URL", "https://sqs.us-east-1.amazonaws.com/123/inventory.fifo")
        monkeypatch.setenv("ROLLBACK_QUEUE_URL", "https://sqs.us-east-1.amazonaws.com/123/rollback.fifo")
        monkeypatch.setenv("STATUS_QUEUE_URL", "https://sqs.us-east-1.amazonaws.com/123/status.fifo")
        monkeypatch.setenv("DATABASE_URL", "postgresql://user:pass@localhost:5432/testdb")

        settings = get_settings()
        assert isinstance(settings, Settings)
