import os
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

os.environ.setdefault("PAYMENT_QUEUE_URL", "https://sqs.us-east-1.amazonaws.com/123/payment.fifo")
os.environ.setdefault("INVENTORY_QUEUE_URL", "https://sqs.us-east-1.amazonaws.com/123/inventory.fifo")
os.environ.setdefault("ROLLBACK_QUEUE_URL", "https://sqs.us-east-1.amazonaws.com/123/rollback.fifo")
os.environ.setdefault("STATUS_QUEUE_URL", "https://sqs.us-east-1.amazonaws.com/123/status.fifo")
os.environ.setdefault("DATABASE_URL", "postgresql://localhost/test")

from saga_initiator.dependencies import set_dynamodb_table, set_sqs_client
from saga_initiator.main import app


@pytest.fixture
def mock_sqs_client():
    client = MagicMock()
    client.send_message.return_value = {"MessageId": "msg-123"}
    set_sqs_client(client)
    return client


@pytest.fixture
def mock_table():
    table = MagicMock()
    set_dynamodb_table(table)
    return table


@pytest.fixture
def client(mock_sqs_client, mock_table):
    with TestClient(app) as c:
        yield c
