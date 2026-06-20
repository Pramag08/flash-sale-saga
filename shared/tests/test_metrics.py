from unittest.mock import MagicMock

import pytest
from shared.metrics import (
    BUFFER_SIZE,
    NAMESPACE,
    clear_metric_buffer,
    emit_metric,
    flush_metrics,
    set_cloudwatch_client,
)


@pytest.fixture(autouse=True)
def reset_metrics():
    clear_metric_buffer()
    yield
    clear_metric_buffer()


@pytest.fixture
def mock_cloudwatch_client():
    client = MagicMock()
    client.put_metric_data.return_value = {}
    set_cloudwatch_client(client)
    return client


class TestEmitMetric:
    def test_single_metric_buffered(self, mock_cloudwatch_client):
        emit_metric("TestMetric", 1.0, "Count")
        mock_cloudwatch_client.put_metric_data.assert_not_called()

    def test_flush_at_buffer_size(self, mock_cloudwatch_client):
        for i in range(BUFFER_SIZE):
            emit_metric(f"Metric{i}", 1.0, "Count")

        mock_cloudwatch_client.put_metric_data.assert_called_once()

        call_kwargs = mock_cloudwatch_client.put_metric_data.call_args[1]
        assert call_kwargs["Namespace"] == NAMESPACE
        assert len(call_kwargs["MetricData"]) == BUFFER_SIZE

    def test_metric_with_dimensions(self, mock_cloudwatch_client):
        emit_metric(
            "TestMetric",
            1.0,
            "Count",
            dimensions={"event_id": "evt-123", "tier_name": "VIP"},
        )
        flush_metrics()

        call_kwargs = mock_cloudwatch_client.put_metric_data.call_args[1]
        metric_data = call_kwargs["MetricData"][0]
        assert metric_data["MetricName"] == "TestMetric"
        assert metric_data["Value"] == 1.0
        assert metric_data["Unit"] == "Count"
        assert len(metric_data["Dimensions"]) == 2

        dimension_dict = {d["Name"]: d["Value"] for d in metric_data["Dimensions"]}
        assert dimension_dict["event_id"] == "evt-123"
        assert dimension_dict["tier_name"] == "VIP"

    def test_metric_without_dimensions(self, mock_cloudwatch_client):
        emit_metric("SimpleMetric", 5.0, "Milliseconds")
        flush_metrics()

        call_kwargs = mock_cloudwatch_client.put_metric_data.call_args[1]
        metric_data = call_kwargs["MetricData"][0]
        assert "Dimensions" not in metric_data

    def test_custom_value_and_unit(self, mock_cloudwatch_client):
        emit_metric("LatencyMetric", 45.5, "Milliseconds")
        flush_metrics()

        call_kwargs = mock_cloudwatch_client.put_metric_data.call_args[1]
        metric_data = call_kwargs["MetricData"][0]
        assert metric_data["Value"] == 45.5
        assert metric_data["Unit"] == "Milliseconds"


class TestFlushMetrics:
    def test_flush_empty_buffer(self, mock_cloudwatch_client):
        flush_metrics()
        mock_cloudwatch_client.put_metric_data.assert_not_called()

    def test_flush_clears_buffer(self, mock_cloudwatch_client):
        emit_metric("Metric1", 1.0, "Count")
        emit_metric("Metric2", 2.0, "Count")
        flush_metrics()

        assert mock_cloudwatch_client.put_metric_data.call_count == 1

        emit_metric("Metric3", 3.0, "Count")
        flush_metrics()

        assert mock_cloudwatch_client.put_metric_data.call_count == 2
        call_kwargs = mock_cloudwatch_client.put_metric_data.call_args[1]
        assert len(call_kwargs["MetricData"]) == 1

    def test_flush_handles_error_gracefully(self, mock_cloudwatch_client):
        mock_cloudwatch_client.put_metric_data.side_effect = Exception("AWS Error")

        emit_metric("TestMetric", 1.0, "Count")
        flush_metrics()

        mock_cloudwatch_client.put_metric_data.assert_called_once()


class TestClearMetricBuffer:
    def test_clear_buffer(self, mock_cloudwatch_client):
        emit_metric("Metric1", 1.0, "Count")
        emit_metric("Metric2", 2.0, "Count")

        clear_metric_buffer()
        flush_metrics()

        mock_cloudwatch_client.put_metric_data.assert_not_called()
