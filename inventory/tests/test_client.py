import boto3
import pytest
from inventory.db.client import DynamoDBClient
from moto import mock_aws


@pytest.fixture
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    import os

    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
    os.environ.pop("AWS_ENDPOINT_URL", None)


@pytest.fixture
def dynamodb_table(aws_credentials):
    """Create a mock DynamoDB table."""
    with mock_aws():
        dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
        table = dynamodb.create_table(
            TableName="FlashSaleInventory",
            KeySchema=[
                {"AttributeName": "PK", "KeyType": "HASH"},
                {"AttributeName": "SK", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "PK", "AttributeType": "S"},
                {"AttributeName": "SK", "AttributeType": "S"},
                {"AttributeName": "GSI1PK", "AttributeType": "S"},
                {"AttributeName": "GSI1SK", "AttributeType": "S"},
            ],
            GlobalSecondaryIndexes=[
                {
                    "IndexName": "GSI1",
                    "KeySchema": [
                        {"AttributeName": "GSI1PK", "KeyType": "HASH"},
                        {"AttributeName": "GSI1SK", "KeyType": "RANGE"},
                    ],
                    "Projection": {"ProjectionType": "ALL"},
                }
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        table.wait_until_exists()
        yield table


@pytest.fixture
def client(dynamodb_table):
    """Create DynamoDBClient with mock table."""
    return DynamoDBClient(table_name="FlashSaleInventory", dynamodb_resource=boto3.resource("dynamodb"))


class TestConditionalDecrement:
    def test_successful_decrement(self, dynamodb_table, client):
        dynamodb_table.put_item(
            Item={
                "PK": "EVENT#test-event",
                "SK": "TIER#VIP",
                "available_qty": 10,
                "updated_at": "2026-01-01T00:00:00Z",
            }
        )

        result = client.conditional_decrement("EVENT#test-event", "TIER#VIP", 2)
        assert result is True

        response = dynamodb_table.get_item(Key={"PK": "EVENT#test-event", "SK": "TIER#VIP"})
        assert response["Item"]["available_qty"] == 8

    def test_decrement_fails_insufficient_quantity(self, dynamodb_table, client):
        dynamodb_table.put_item(
            Item={
                "PK": "EVENT#test-event",
                "SK": "TIER#VIP",
                "available_qty": 1,
                "updated_at": "2026-01-01T00:00:00Z",
            }
        )

        result = client.conditional_decrement("EVENT#test-event", "TIER#VIP", 5)
        assert result is False

        response = dynamodb_table.get_item(Key={"PK": "EVENT#test-event", "SK": "TIER#VIP"})
        assert response["Item"]["available_qty"] == 1

    def test_decrement_exact_quantity(self, dynamodb_table, client):
        dynamodb_table.put_item(
            Item={
                "PK": "EVENT#test-event",
                "SK": "TIER#VIP",
                "available_qty": 5,
                "updated_at": "2026-01-01T00:00:00Z",
            }
        )

        result = client.conditional_decrement("EVENT#test-event", "TIER#VIP", 5)
        assert result is True

        response = dynamodb_table.get_item(Key={"PK": "EVENT#test-event", "SK": "TIER#VIP"})
        assert response["Item"]["available_qty"] == 0


class TestPutReservation:
    def test_put_reservation(self, dynamodb_table, client):
        result = client.put_reservation(
            transaction_id="txn-123",
            event_id="evt-456",
            tier_name="VIP",
            user_id="user-789",
            quantity=2,
            payment_reference="pay-abc",
            ttl_minutes=15,
        )

        assert result is True

        response = dynamodb_table.get_item(Key={"PK": "TXN#txn-123", "SK": "RESERVATION"})
        item = response["Item"]
        assert item["PK"] == "TXN#txn-123"
        assert item["SK"] == "RESERVATION"
        assert item["entity_type"] == "RESERVATION"
        assert item["event_id"] == "evt-456"
        assert item["tier_name"] == "VIP"
        assert item["user_id"] == "user-789"
        assert item["quantity"] == 2
        assert item["status"] == "RESERVED"
        assert item["payment_reference"] == "pay-abc"
        assert "reserved_at" in item
        assert "ttl" in item
        assert item["GSI1PK"] == "USER#user-789"
        assert item["GSI1SK"] == "TXN#txn-123"

    def test_put_reservation_fails_if_already_exists(self, dynamodb_table, client):
        client.put_reservation(
            transaction_id="txn-123",
            event_id="evt-456",
            tier_name="VIP",
            user_id="user-789",
            quantity=2,
            payment_reference="pay-abc",
        )

        result = client.put_reservation(
            transaction_id="txn-123",
            event_id="evt-456",
            tier_name="VIP",
            user_id="user-789",
            quantity=2,
            payment_reference="pay-abc",
        )

        assert result is False


class TestFindActiveReservation:
    def test_finds_reserved_reservation(self, dynamodb_table, client):
        dynamodb_table.put_item(
            Item={
                "PK": "TXN#txn-123",
                "SK": "RESERVATION",
                "GSI1PK": "USER#user-789",
                "GSI1SK": "TXN#txn-123",
                "entity_type": "RESERVATION",
                "event_id": "evt-456",
                "status": "RESERVED",
            }
        )

        results = client.find_active_reservation("user-789", "evt-456")
        assert len(results) == 1
        assert results[0]["status"] == "RESERVED"

    def test_finds_confirmed_reservation(self, dynamodb_table, client):
        dynamodb_table.put_item(
            Item={
                "PK": "TXN#txn-123",
                "SK": "RESERVATION",
                "GSI1PK": "USER#user-789",
                "GSI1SK": "TXN#txn-123",
                "entity_type": "RESERVATION",
                "event_id": "evt-456",
                "status": "CONFIRMED",
            }
        )

        results = client.find_active_reservation("user-789", "evt-456")
        assert len(results) == 1
        assert results[0]["status"] == "CONFIRMED"

    def test_ignores_cancelled_reservations(self, dynamodb_table, client):
        dynamodb_table.put_item(
            Item={
                "PK": "TXN#txn-123",
                "SK": "RESERVATION",
                "GSI1PK": "USER#user-789",
                "GSI1SK": "TXN#txn-123",
                "entity_type": "RESERVATION",
                "event_id": "evt-456",
                "status": "CANCELLED",
            }
        )

        results = client.find_active_reservation("user-789", "evt-456")
        assert results == []

    def test_returns_empty_when_no_reservation(self, client):
        results = client.find_active_reservation("nonexistent", "nonexistent")
        assert results == []


class TestGetReservation:
    def test_get_existing_reservation(self, dynamodb_table, client):
        dynamodb_table.put_item(
            Item={
                "PK": "TXN#txn-123",
                "SK": "RESERVATION",
                "entity_type": "RESERVATION",
                "event_id": "evt-456",
                "tier_name": "VIP",
                "user_id": "user-789",
                "quantity": 2,
                "status": "RESERVED",
            }
        )

        result = client.get_reservation("txn-123")
        assert result is not None
        assert result["PK"] == "TXN#txn-123"
        assert result["status"] == "RESERVED"

    def test_get_nonexistent_reservation(self, client):
        result = client.get_reservation("nonexistent")
        assert result is None


class TestGetEventMetadata:
    def test_get_existing_event(self, dynamodb_table, client):
        dynamodb_table.put_item(
            Item={
                "PK": "EVENT#evt-123",
                "SK": "METADATA",
                "entity_type": "EVENT",
                "sale_status": "ACTIVE",
            }
        )

        result = client.get_event_metadata("evt-123")
        assert result is not None
        assert result["sale_status"] == "ACTIVE"

    def test_get_nonexistent_event(self, client):
        result = client.get_event_metadata("nonexistent")
        assert result is None


class TestRestoreQuantity:
    def test_restore_quantity(self, dynamodb_table, client):
        dynamodb_table.put_item(
            Item={
                "PK": "EVENT#test-event",
                "SK": "TIER#VIP",
                "available_qty": 5,
                "updated_at": "2026-01-01T00:00:00Z",
            }
        )

        client.restore_quantity("test-event", "VIP", 3)

        response = dynamodb_table.get_item(Key={"PK": "EVENT#test-event", "SK": "TIER#VIP"})
        assert response["Item"]["available_qty"] == 8
