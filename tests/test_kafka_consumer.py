import json
import threading

import pytest

from src import kafka_consumer
from src.kafka_consumer import _extract_alert, consume
from src.models import DeliveryResult
from src.state import ConsumerState


class FakeMessage:
    def __init__(self, value: bytes, offset: int):
        self._value = value
        self._offset = offset

    def value(self):
        return self._value

    def offset(self):
        return self._offset

    def topic(self):
        return "keep-dlq"

    def partition(self):
        return 0

    def error(self):
        return None


class FakeConsumer:
    """Hands out the queued messages, then sets the shutdown event."""

    def __init__(self, messages, shutdown_event):
        self.all_messages = {m.offset(): m for m in messages}
        self.messages = list(messages)
        self.shutdown_event = shutdown_event
        self.committed = []
        self.seeks = []
        self.closed = False

    def subscribe(self, topics):
        pass

    def poll(self, timeout=None):
        if not self.messages:
            self.shutdown_event.set()
            return None
        return self.messages.pop(0)

    def commit(self, msg):
        self.committed.append(msg.offset())

    def seek(self, partition):
        # Like Kafka: the next poll() starts again from the given offset.
        self.seeks.append(partition.offset)
        self.messages = [
            m for offset, m in sorted(self.all_messages.items())
            if offset >= partition.offset
        ]

    def assignment(self):
        return []

    def close(self):
        self.closed = True


class FakeKeepClient:
    def __init__(self, results):
        self.results = list(results)
        self.sent = []

    def send(self, alert):
        self.sent.append(alert)
        return self.results.pop(0)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def message(payload, offset):
    raw = payload if isinstance(payload, bytes) else json.dumps(payload).encode()
    return FakeMessage(raw, offset)


@pytest.fixture
def run_consume(monkeypatch):
    def _run(messages, results=()):
        shutdown_event = threading.Event()
        fake_consumer = FakeConsumer(messages, shutdown_event)
        fake_keep = FakeKeepClient(results)
        state = ConsumerState()
        monkeypatch.setattr(kafka_consumer, "Consumer", lambda _conf: fake_consumer)
        monkeypatch.setattr(kafka_consumer, "KeepClient", lambda: fake_keep)
        consume(shutdown_event, state)
        return fake_consumer, fake_keep, state

    return _run


class TestExtractAlert:
    def test_ingestion_contract(self):
        alert, error, fingerprint = _extract_alert(
            {"alert": {"fingerprint": "f"}, "error": {"reason": "x"}}
        )
        assert alert == {"fingerprint": "f"}
        assert error == {"reason": "x"}
        assert fingerprint == "f"

    def test_gateway_fallback(self):
        alert, error, fingerprint = _extract_alert(
            {"event": {"fingerprint": "f"}, "event_type": "alert"}
        )
        assert alert == {"fingerprint": "f"}
        assert error == {}
        assert fingerprint == "f"

    def test_bare_alert(self):
        alert, _, fingerprint = _extract_alert({"fingerprint": "f"})
        assert alert == {"fingerprint": "f"}
        assert fingerprint == "f"

    def test_non_dict_alert_has_no_fingerprint(self):
        alert, _, fingerprint = _extract_alert({"alert": ["not", "a", "dict"]})
        assert alert == ["not", "a", "dict"]
        assert fingerprint is None


class TestConsume:
    def test_success_commits(self, run_consume):
        consumer, keep, state = run_consume(
            [message({"alert": {"fingerprint": "f"}}, 0)], [DeliveryResult.SUCCESS]
        )
        assert keep.sent == [{"fingerprint": "f"}]
        assert consumer.committed == [0]
        assert state.get_counters()["success_total"] == 1
        assert consumer.closed

    def test_terminal_commits(self, run_consume):
        consumer, _, state = run_consume(
            [message({"alert": {"fingerprint": "f"}}, 0)], [DeliveryResult.TERMINAL]
        )
        assert consumer.committed == [0]
        assert state.get_counters()["terminal_total"] == 1

    def test_retryable_exhausted_retries_same_alert(self, run_consume):
        consumer, keep, state = run_consume(
            [
                message({"alert": {"fingerprint": "f0"}}, 0),
                message({"alert": {"fingerprint": "f1"}}, 1),
            ],
            [
                DeliveryResult.RETRYABLE_EXHAUSTED,
                DeliveryResult.SUCCESS,
                DeliveryResult.SUCCESS,
            ],
        )
        # f0 is rewound and sent again instead of being skipped by f1's commit.
        assert [a["fingerprint"] for a in keep.sent] == ["f0", "f0", "f1"]
        assert consumer.seeks == [0]
        assert consumer.committed == [0, 1]
        assert state.get_counters()["retryable_exhausted_total"] == 1

    @pytest.mark.parametrize(
        "raw", [b"not json", json.dumps([1, 2]).encode(), json.dumps({"alert": [1]}).encode()]
    )
    def test_undecodable_messages_are_skipped(self, run_consume, raw):
        consumer, keep, state = run_consume([message(raw, 0)])
        assert keep.sent == []
        assert consumer.committed == [0]
        assert state.get_counters()["decode_errors_total"] == 1

    def test_backoff_clears_after_it_ends(self, run_consume, monkeypatch):
        monkeypatch.setattr(kafka_consumer.settings, "keep_max_consecutive_failures", 2)
        monkeypatch.setattr(kafka_consumer.settings, "keep_backoff_seconds", 0)
        _, _, state = run_consume(
            [message({"alert": {"fingerprint": f"f{i}"}}, i) for i in range(2)],
            [DeliveryResult.TERMINAL, DeliveryResult.TERMINAL],
        )
        assert state.get_counters()["backoffs_total"] == 1
        assert not state.is_in_backoff()
