import json
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError
from pydantic import BaseModel
from shared.sqs_client import MAX_RETRIES, send_fifo_message


class MockEvent(BaseModel):
    event_type: str = "TestEvent"
    transaction_id: str = "txn-123"
    amount: int = 100


@pytest.fixture
def mock_sqs_client():
    return MagicMock()


@pytest.fixture
def sample_event():
    return MockEvent()


class TestSendFifoMessage:
    def test_successful_send(self, mock_sqs_client, sample_event):
        mock_sqs_client.send_message.return_value = {"MessageId": "msg-abc-123"}

        message_id = send_fifo_message(
            queue_url="https://sqs.us-east-1.amazonaws.com/123/test.fifo",
            event=sample_event,
            message_group_id="group-1",
            dedup_id="dedup-1",
            sqs_client=mock_sqs_client,
        )

        assert message_id == "msg-abc-123"
        mock_sqs_client.send_message.assert_called_once()

        call_kwargs = mock_sqs_client.send_message.call_args[1]
        assert call_kwargs["MessageGroupId"] == "group-1"
        assert call_kwargs["MessageDeduplicationId"] == "dedup-1"

        body = json.loads(call_kwargs["MessageBody"])
        assert body["event_type"] == "TestEvent"
        assert body["transaction_id"] == "txn-123"

    def test_serializes_pydantic_event_to_json(self, mock_sqs_client, sample_event):
        mock_sqs_client.send_message.return_value = {"MessageId": "msg-123"}

        send_fifo_message(
            queue_url="https://sqs.us-east-1.amazonaws.com/123/test.fifo",
            event=sample_event,
            message_group_id="group-1",
            dedup_id="dedup-1",
            sqs_client=mock_sqs_client,
        )

        call_kwargs = mock_sqs_client.send_message.call_args[1]
        body = json.loads(call_kwargs["MessageBody"])
        assert isinstance(body, dict)
        assert body["event_type"] == "TestEvent"
        assert body["amount"] == 100

    def test_retry_on_throttling_exception(self, mock_sqs_client, sample_event):
        error_response = {"Error": {"Code": "ThrottlingException", "Message": "Rate exceeded"}}
        mock_sqs_client.send_message.side_effect = [
            ClientError(error_response, "SendMessage"),
            ClientError(error_response, "SendMessage"),
            {"MessageId": "msg-success"},
        ]

        with patch("shared.sqs_client.time.sleep"):
            message_id = send_fifo_message(
                queue_url="https://sqs.us-east-1.amazonaws.com/123/test.fifo",
                event=sample_event,
                message_group_id="group-1",
                dedup_id="dedup-1",
                sqs_client=mock_sqs_client,
            )

        assert message_id == "msg-success"
        assert mock_sqs_client.send_message.call_count == 3

    def test_retry_on_service_unavailable(self, mock_sqs_client, sample_event):
        error_response = {"Error": {"Code": "ServiceUnavailable", "Message": "Service down"}}
        mock_sqs_client.send_message.side_effect = [
            ClientError(error_response, "SendMessage"),
            {"MessageId": "msg-success"},
        ]

        with patch("shared.sqs_client.time.sleep"):
            message_id = send_fifo_message(
                queue_url="https://sqs.us-east-1.amazonaws.com/123/test.fifo",
                event=sample_event,
                message_group_id="group-1",
                dedup_id="dedup-1",
                sqs_client=mock_sqs_client,
            )

        assert message_id == "msg-success"
        assert mock_sqs_client.send_message.call_count == 2

    def test_no_retry_on_non_retryable_error(self, mock_sqs_client, sample_event):
        error_response = {"Error": {"Code": "InvalidParameterValue", "Message": "Bad param"}}
        mock_sqs_client.send_message.side_effect = ClientError(error_response, "SendMessage")

        with pytest.raises(ClientError) as exc_info:
            send_fifo_message(
                queue_url="https://sqs.us-east-1.amazonaws.com/123/test.fifo",
                event=sample_event,
                message_group_id="group-1",
                dedup_id="dedup-1",
                sqs_client=mock_sqs_client,
            )

        assert exc_info.value.response["Error"]["Code"] == "InvalidParameterValue"
        assert mock_sqs_client.send_message.call_count == 1

    def test_max_retries_exceeded(self, mock_sqs_client, sample_event):
        error_response = {"Error": {"Code": "ThrottlingException", "Message": "Rate exceeded"}}
        mock_sqs_client.send_message.side_effect = ClientError(error_response, "SendMessage")

        with patch("shared.sqs_client.time.sleep"), pytest.raises(ClientError):
            send_fifo_message(
                queue_url="https://sqs.us-east-1.amazonaws.com/123/test.fifo",
                event=sample_event,
                message_group_id="group-1",
                dedup_id="dedup-1",
                sqs_client=mock_sqs_client,
            )

        assert mock_sqs_client.send_message.call_count == MAX_RETRIES

    def test_exponential_backoff_timing(self, mock_sqs_client, sample_event):
        error_response = {"Error": {"Code": "ThrottlingException", "Message": "Rate exceeded"}}
        mock_sqs_client.send_message.side_effect = [
            ClientError(error_response, "SendMessage"),
            ClientError(error_response, "SendMessage"),
            {"MessageId": "msg-success"},
        ]

        with patch("shared.sqs_client.time.sleep") as mock_sleep:
            send_fifo_message(
                queue_url="https://sqs.us-east-1.amazonaws.com/123/test.fifo",
                event=sample_event,
                message_group_id="group-1",
                dedup_id="dedup-1",
                sqs_client=mock_sqs_client,
            )

        assert mock_sleep.call_count == 2
        mock_sleep.assert_any_call(0.1)
        mock_sleep.assert_any_call(0.2)

    def test_uses_boto3_client_when_not_provided(self, sample_event):
        with patch("shared.sqs_client.boto3.client") as mock_boto3_client:
            mock_client = MagicMock()
            mock_client.send_message.return_value = {"MessageId": "msg-123"}
            mock_boto3_client.return_value = mock_client

            message_id = send_fifo_message(
                queue_url="https://sqs.us-east-1.amazonaws.com/123/test.fifo",
                event=sample_event,
                message_group_id="group-1",
                dedup_id="dedup-1",
            )

            mock_boto3_client.assert_called_once_with("sqs")
            assert message_id == "msg-123"
