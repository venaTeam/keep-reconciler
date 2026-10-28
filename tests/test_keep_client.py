import httpx
import pytest

from src import keep_client
from src.keep_client import KeepClient, _preserve_last_received
from src.models import DeliveryResult


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    monkeypatch.setattr(keep_client.time, "sleep", lambda _: None)


def make_client(handler) -> KeepClient:
    client = KeepClient()
    client._client = httpx.Client(transport=httpx.MockTransport(handler))
    return client


class TestPreserveLastReceived:
    def test_keeps_existing_last_received(self):
        alert = {"lastReceived": "2024-01-01T00:00:00Z", "timestamp": "other"}
        assert _preserve_last_received(alert) == alert

    def test_normalises_snake_case(self):
        result = _preserve_last_received({"last_received": "t1"})
        assert result["lastReceived"] == "t1"

    @pytest.mark.parametrize(
        "source_key", ["timestamp", "timeCreated", "startedAt", "firingStartTime"]
    )
    def test_falls_back_to_event_timestamp(self, source_key):
        result = _preserve_last_received({source_key: "t1"})
        assert result["lastReceived"] == "t1"

    def test_does_not_mutate_input(self):
        alert = {"timestamp": "t1"}
        _preserve_last_received(alert)
        assert "lastReceived" not in alert

    def test_no_fallback_leaves_alert_unchanged(self):
        assert _preserve_last_received({"fingerprint": "f"}) == {"fingerprint": "f"}


class TestSend:
    def test_success(self):
        requests = []

        def handler(request):
            requests.append(request)
            return httpx.Response(202)

        result = make_client(handler).send({"fingerprint": "f", "timestamp": "t1"})

        assert result == DeliveryResult.SUCCESS
        assert len(requests) == 1
        assert str(requests[0].url) == "http://keep.test/alerts/event"
        assert requests[0].headers["X-API-KEY"] == "test-key"
        assert b'"lastReceived":"t1"' in requests[0].content.replace(b" ", b"")

    def test_client_error_is_terminal_without_retry(self):
        calls = []

        def handler(request):
            calls.append(request)
            return httpx.Response(400, text="bad alert")

        assert make_client(handler).send({"fingerprint": "f"}) == DeliveryResult.TERMINAL
        assert len(calls) == 1

    @pytest.mark.parametrize("status_code", [429, 500, 503])
    def test_retryable_status_exhausts_retries(self, status_code):
        calls = []

        def handler(request):
            calls.append(request)
            return httpx.Response(status_code)

        result = make_client(handler).send({"fingerprint": "f"})

        assert result == DeliveryResult.RETRYABLE_EXHAUSTED
        assert len(calls) == keep_client.settings.keep_send_retries

    def test_network_error_is_retried(self):
        calls = []

        def handler(request):
            calls.append(request)
            if len(calls) == 1:
                raise httpx.ConnectError("boom")
            return httpx.Response(200)

        assert make_client(handler).send({"fingerprint": "f"}) == DeliveryResult.SUCCESS
        assert len(calls) == 2
