import uuid
from datetime import UTC, datetime, timedelta


def _reservation_item(status="RESERVED", reserved_at=None, **overrides) -> dict:
    if reserved_at is None:
        reserved_at = datetime.now(UTC).isoformat()
    item = {
        "PK": f"TXN#{uuid.uuid4()}",
        "SK": "RESERVATION",
        "status": status,
        "event_id": str(uuid.uuid4()),
        "tier_name": "VIP",
        "quantity": 1,
        "reserved_at": reserved_at,
    }
    item.update(overrides)
    return item


class TestGetSagaStatus:
    def test_reservation_found(self, client, mock_table):
        txn_id = uuid.uuid4()
        mock_table.get_item.return_value = {"Item": _reservation_item(status="RESERVED")}

        response = client.get(f"/status/{txn_id}")

        assert response.status_code == 200
        data = response.json()
        assert data["transaction_id"] == str(txn_id)
        assert data["status"] == "RESERVED"
        assert data["event_id"] is not None
        assert data["tier_name"] == "VIP"
        assert data["quantity"] == 1

    def test_confirmed_status(self, client, mock_table):
        txn_id = uuid.uuid4()
        mock_table.get_item.return_value = {"Item": _reservation_item(status="CONFIRMED")}

        response = client.get(f"/status/{txn_id}")

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "CONFIRMED"

    def test_cancelled_status(self, client, mock_table):
        txn_id = uuid.uuid4()
        mock_table.get_item.return_value = {"Item": _reservation_item(status="CANCELLED")}

        response = client.get(f"/status/{txn_id}")

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "CANCELLED"

    def test_not_found_returns_404(self, client, mock_table):
        txn_id = uuid.uuid4()
        mock_table.get_item.return_value = {}

        response = client.get(f"/status/{txn_id}")

        assert response.status_code == 404

    def test_timed_out_reservation(self, client, mock_table):
        txn_id = uuid.uuid4()
        old_time = (datetime.now(UTC) - timedelta(minutes=15)).isoformat()
        mock_table.get_item.return_value = {"Item": _reservation_item(status="RESERVED", reserved_at=old_time)}

        response = client.get(f"/status/{txn_id}")

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "TIMED_OUT"

    def test_recent_reservation_not_timed_out(self, client, mock_table):
        txn_id = uuid.uuid4()
        recent_time = (datetime.now(UTC) - timedelta(minutes=5)).isoformat()
        mock_table.get_item.return_value = {"Item": _reservation_item(status="RESERVED", reserved_at=recent_time)}

        response = client.get(f"/status/{txn_id}")

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "RESERVED"

    def test_invalid_transaction_id_returns_422(self, client, mock_table):
        response = client.get("/status/not-a-uuid")

        assert response.status_code == 422

    def test_dynamodb_error_returns_503(self, client, mock_table):
        from botocore.exceptions import ClientError

        mock_table.get_item.side_effect = ClientError(
            {"Error": {"Code": "InternalServerError", "Message": "DynamoDB down"}},
            "GetItem",
        )
        txn_id = uuid.uuid4()

        response = client.get(f"/status/{txn_id}")

        assert response.status_code == 503
