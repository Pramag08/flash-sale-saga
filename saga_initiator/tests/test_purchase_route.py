import json
import uuid

from botocore.exceptions import ClientError


def _valid_purchase_body(**overrides) -> dict:
    base = {
        "user_id": str(uuid.uuid4()),
        "event_id": str(uuid.uuid4()),
        "tier_name": "VIP",
        "quantity": 1,
    }
    base.update(overrides)
    return base


def _tier_item(price_cents=4999, available_qty=100) -> dict:
    return {
        "PK": f"EVENT#{uuid.uuid4()}",
        "SK": "TIER#VIP",
        "price_cents": price_cents,
        "available_qty": available_qty,
        "tier_name": "VIP",
    }


def _conditional_check_failed_error() -> ClientError:
    return ClientError(
        {"Error": {"Code": "ConditionalCheckFailedException", "Message": "Conditional check failed"}},
        "PutItem",
    )


class TestInitiatePurchase:
    def test_happy_path(self, client, mock_sqs_client, mock_table):
        mock_table.get_item.return_value = {"Item": _tier_item()}

        body = _valid_purchase_body()
        response = client.post("/buy-ticket", json=body)

        assert response.status_code == 202
        data = response.json()
        assert "transaction_id" in data
        assert data["status"] == "ACCEPTED"
        assert "message" in data

        mock_sqs_client.send_message.assert_called_once()
        call_kwargs = mock_sqs_client.send_message.call_args[1]
        assert call_kwargs["QueueUrl"] == "https://sqs.us-east-1.amazonaws.com/123/payment.fifo"
        msg_body = json.loads(call_kwargs["MessageBody"])
        assert msg_body["event_type"] == "ProcessPayment"
        assert msg_body["tier_name"] == "VIP"
        assert msg_body["quantity"] == 1
        assert msg_body["amount_cents"] == 4999
        assert msg_body["currency"] == "USD"

    def test_amount_cents_calculated_for_multiple_quantity(self, client, mock_sqs_client, mock_table):
        mock_table.get_item.return_value = {"Item": _tier_item(price_cents=5000)}

        body = _valid_purchase_body(quantity=3)
        response = client.post("/buy-ticket", json=body)

        assert response.status_code == 202
        call_kwargs = mock_sqs_client.send_message.call_args[1]
        msg_body = json.loads(call_kwargs["MessageBody"])
        assert msg_body["amount_cents"] == 15000

    def test_duplicate_purchase_returns_409(self, client, mock_sqs_client, mock_table):
        existing_txn = str(uuid.uuid4())
        mock_table.put_item.side_effect = _conditional_check_failed_error()
        mock_table.get_item.return_value = {"Item": {"PK": "USERLOCK#...", "transaction_id": existing_txn}}

        body = _valid_purchase_body()
        response = client.post("/buy-ticket", json=body)

        assert response.status_code == 409
        detail = response.json()["detail"]
        assert detail["existing_transaction_id"] == existing_txn
        mock_sqs_client.send_message.assert_not_called()

    def test_tier_not_found_returns_404(self, client, mock_sqs_client, mock_table):
        mock_table.get_item.return_value = {}

        body = _valid_purchase_body()
        response = client.post("/buy-ticket", json=body)

        assert response.status_code == 404
        mock_sqs_client.send_message.assert_not_called()
        mock_table.delete_item.assert_called_once()

    def test_sold_out_returns_409(self, client, mock_sqs_client, mock_table):
        mock_table.get_item.return_value = {"Item": _tier_item(available_qty=0)}

        body = _valid_purchase_body()
        response = client.post("/buy-ticket", json=body)

        assert response.status_code == 409
        mock_sqs_client.send_message.assert_not_called()
        mock_table.delete_item.assert_called_once()

    def test_sqs_failure_returns_503(self, client, mock_sqs_client, mock_table):
        mock_table.get_item.return_value = {"Item": _tier_item()}
        mock_sqs_client.send_message.side_effect = Exception("SQS down")

        body = _valid_purchase_body()
        response = client.post("/buy-ticket", json=body)

        assert response.status_code == 503
        mock_table.delete_item.assert_called_once()

    def test_invalid_user_id_returns_422(self, client, mock_sqs_client, mock_table):
        body = _valid_purchase_body(user_id="not-a-uuid")
        response = client.post("/buy-ticket", json=body)

        assert response.status_code == 422

    def test_invalid_quantity_zero_returns_422(self, client, mock_sqs_client, mock_table):
        body = _valid_purchase_body(quantity=0)
        response = client.post("/buy-ticket", json=body)

        assert response.status_code == 422

    def test_invalid_quantity_negative_returns_422(self, client, mock_sqs_client, mock_table):
        body = _valid_purchase_body(quantity=-1)
        response = client.post("/buy-ticket", json=body)

        assert response.status_code == 422

    def test_missing_tier_name_returns_422(self, client, mock_sqs_client, mock_table):
        body = _valid_purchase_body()
        del body["tier_name"]
        response = client.post("/buy-ticket", json=body)

        assert response.status_code == 422

    def test_sqs_message_group_id_is_transaction_id(self, client, mock_sqs_client, mock_table):
        mock_table.get_item.return_value = {"Item": _tier_item()}

        body = _valid_purchase_body()
        response = client.post("/buy-ticket", json=body)

        assert response.status_code == 202
        call_kwargs = mock_sqs_client.send_message.call_args[1]
        msg_body = json.loads(call_kwargs["MessageBody"])
        assert call_kwargs["MessageGroupId"] == msg_body["transaction_id"]
        assert call_kwargs["MessageDeduplicationId"] == msg_body["idempotency_key"]

    def test_lock_acquisition_failure_returns_503(self, client, mock_sqs_client, mock_table):
        mock_table.put_item.side_effect = ClientError(
            {"Error": {"Code": "ProvisionedThroughputExceededException", "Message": "throttled"}},
            "PutItem",
        )

        body = _valid_purchase_body()
        response = client.post("/buy-ticket", json=body)

        assert response.status_code == 503
        mock_sqs_client.send_message.assert_not_called()

    def test_happy_path_does_not_release_lock(self, client, mock_sqs_client, mock_table):
        mock_table.get_item.return_value = {"Item": _tier_item()}

        body = _valid_purchase_body()
        response = client.post("/buy-ticket", json=body)

        assert response.status_code == 202
        mock_table.delete_item.assert_not_called()
