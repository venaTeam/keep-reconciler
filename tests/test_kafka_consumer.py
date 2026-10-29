import json
import threading
import time

import pytest

from src import kafka_consumer
from src.kafka_consumer import _extract_alert, _poll_paused, _wait_for_replay, consume
from src.models import DeliveryResult
from src.state import ConsumerState


class FakeMessage:
    def __init__(self, value: bytes, offset: int, timestamp=(0, 0)):
        self._value = value
        self._offset = offset
        self._timestamp = timestamp

    def timestamp(self):
        return self._timestamp

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
        self.paused = []
        self.resumed = []
        self.closed = False

    def subscribe(self, topics, on_assign=None):
        if on_assign:
            on_assign(self, [])

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
        return ["p0"]

    def pause(self, partitions):
        self.paused.append(partitions)

    def resume(self, partitions):
        self.resumed.append(partitions)

    def close(self):
        self.closed = True


class FakeKeepClient:
    def __init__(self, results):
        self.results = list(results)
        self.sent = []
        self.replays = []

    def send(self, alert, replay=1):
        self.sent.append(alert)
        self.replays.append(replay)
        return self.results.pop(0)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def message(payload, offset, timestamp=(0, 0)):
    raw = payload if isinstance(payload, bytes) else json.dumps(payload).encode()
    return FakeMessage(raw, offset, timestamp)


@pytest.fixture
def run_consume(monkeypatch):
    def _run(messages, results=(), state=None):
        shutdown_event = threading.Event()
        fake_consumer = FakeConsumer(messages, shutdown_event)
        fake_keep = FakeKeepClient(results)
        state = state or ConsumerState()
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

    def test_batch_is_sent_and_committed(self, run_consume):
        batch = [{"fingerprint": "a"}, {"fingerprint": "b"}]
        consumer, keep, state = run_consume(
            [message({"alert": batch}, 0)], [DeliveryResult.SUCCESS]
        )
        assert keep.sent == [batch]
        assert consumer.committed == [0]
        assert state.get_counters()["decode_errors_total"] == 0

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


class TestReplays:
    def test_replay_number_is_sent(self, run_consume):
        _, keep, _ = run_consume(
            [message({"alert": {"fingerprint": "f"}, "dlq_replay_count": 2}, 0)],
            [DeliveryResult.SUCCESS],
        )
        assert keep.replays == [3]

    def test_first_replay_without_count(self, run_consume):
        _, keep, _ = run_consume(
            [message({"alert": {"fingerprint": "f"}}, 0)], [DeliveryResult.SUCCESS]
        )
        assert keep.replays == [1]

    def test_dropped_after_max_replays(self, run_consume, monkeypatch):
        monkeypatch.setattr(kafka_consumer.settings, "keep_max_dlq_replays", 3)
        consumer, keep, state = run_consume(
            [
                message({"alert": {"fingerprint": "f0"}, "dlq_replay_count": 3}, 0),
                message({"alert": {"fingerprint": "f1"}, "dlq_replay_count": 2}, 1),
            ],
            [DeliveryResult.SUCCESS],
        )
        # f0 used up its replays: committed without being sent again.
        assert [a["fingerprint"] for a in keep.sent] == ["f1"]
        assert consumer.committed == [0, 1]
        assert state.get_counters()["dropped_total"] == 1

    def test_bad_count_is_treated_as_first_replay(self, run_consume):
        _, keep, _ = run_consume(
            [message({"alert": {"fingerprint": "f"}, "dlq_replay_count": "x"}, 0)],
            [DeliveryResult.SUCCESS],
        )
        assert keep.replays == [1]


class FakeClock:
    def __init__(self, start: float):
        self.now = start

    def time(self):
        # Every check advances the clock, standing in for poll()'s 1s wait.
        self.now += 1
        return self.now


class TestWaitForReplay:
    def _consumer(self):
        return FakeConsumer([], threading.Event())

    def test_old_message_is_not_delayed(self, monkeypatch):
        consumer = self._consumer()
        msg = message({}, 0, timestamp=(1, int((time.time() - 3600) * 1000)))
        assert _wait_for_replay(consumer, msg, 0, threading.Event()) is True
        assert consumer.paused == []

    def test_young_message_waits_paused(self, monkeypatch):
        clock = FakeClock(1000.0)
        monkeypatch.setattr(kafka_consumer.time, "time", clock.time)
        monkeypatch.setattr(kafka_consumer, "_poll_paused", lambda _c: None)
        monkeypatch.setattr(kafka_consumer.settings, "keep_replay_delay_seconds", 10)
        consumer = self._consumer()
        msg = message({}, 0, timestamp=(1, 1000 * 1000))

        assert _wait_for_replay(consumer, msg, 1, threading.Event()) is True
        # Delay doubles per replay: 10s * 2**1 after the DLQ write.
        assert clock.now >= 1000 + 20
        assert consumer.paused == [["p0"]]
        assert consumer.resumed == [["p0"]]

    def test_shutdown_interrupts_wait(self, monkeypatch):
        shutdown = threading.Event()
        shutdown.set()
        consumer = self._consumer()
        msg = message({}, 0, timestamp=(1, int(time.time() * 1000)))

        assert _wait_for_replay(consumer, msg, 0, shutdown) is False
        assert consumer.resumed == [["p0"]]

    def test_no_timestamp_is_not_delayed(self):
        consumer = self._consumer()
        assert _wait_for_replay(consumer, message({}, 0), 0, threading.Event()) is True

    def test_shutdown_during_wait_leaves_alert_uncommitted(self, run_consume, monkeypatch):
        monkeypatch.setattr(kafka_consumer, "_wait_for_replay", lambda *a: False)
        consumer, keep, _ = run_consume([message({"alert": {"fingerprint": "f"}}, 0)])
        assert keep.sent == []
        assert consumer.committed == []


def test_poll_paused_rewinds_a_returned_message():
    consumer = FakeConsumer([message({}, 7)], threading.Event())
    _poll_paused(consumer)
    assert consumer.seeks == [7]


def test_assignment_marks_consumer_healthy_again(run_consume):
    state = ConsumerState()
    state.mark_unhealthy()
    run_consume([], state=state)
    assert state.is_healthy()
