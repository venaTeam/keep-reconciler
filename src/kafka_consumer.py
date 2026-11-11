import json
import logging
import threading
import time
from typing import Any

from confluent_kafka import (
    TIMESTAMP_NOT_AVAILABLE,
    Consumer,
    KafkaError,
    KafkaException,
    TopicPartition,
)

from src.config import settings
from src.keep_client import KeepClient, fingerprint_of
from src.models import DeliveryResult
from src.state import ConsumerState

logger = logging.getLogger("keep-reconciler")

# keep-event-handler failure reasons that fail the same way on every replay.
UNREPLAYABLE_REASONS = {"poison", "malformed"}


def kafka_config() -> dict[str, Any]:
    return {
        "bootstrap.servers": ",".join(settings.kafka_dlq_bootstrap_servers_list),
        "group.id": settings.kafka_dlq_group_id,
        "enable.auto.commit": "false",
        "enable.auto.offset.store": "false",
        "security.protocol": "SASL_PLAINTEXT",
        "sasl.mechanism": "SCRAM-SHA-256",
        "sasl.username": settings.kafka_dlq_sasl_username,
        "sasl.password": settings.kafka_dlq_sasl_password,
        "auto.offset.reset": "earliest",
    }


def commit(consumer: Consumer, msg) -> None:
    last_error: Exception | None = None
    for attempt in range(settings.keep_commit_retries):
        try:
            consumer.commit(msg)
            return
        except Exception as e:
            last_error = e
            logger.warning(
                "Kafka commit attempt failed",
                extra={
                    "attempt": attempt + 1,
                    "max_attempts": settings.keep_commit_retries,
                    "error": str(e),
                },
            )
            if attempt < settings.keep_commit_retries - 1:
                time.sleep(2**attempt)
    logger.error(
        "Kafka commit failed; raising to restart consumer",
        extra={
            "attempts": settings.keep_commit_retries,
            "error_type": type(last_error).__name__ if last_error else "unknown",
            "error": str(last_error) if last_error else "no error captured",
        },
    )
    raise last_error if last_error else RuntimeError("Kafka commit failed")


def _poll_paused(consumer: Consumer) -> None:
    # poll() keeps the consumer in the group while its partitions are paused.
    # A message fetched just before the pause can still come back here: rewind
    # to it so it isn't skipped.
    msg = consumer.poll(timeout=1.0)
    if msg is not None and not msg.error():
        consumer.seek(TopicPartition(msg.topic(), msg.partition(), msg.offset()))


def backoff(consumer: Consumer, shutdown_event: threading.Event) -> None:
    partitions = consumer.assignment() or []
    if partitions:
        consumer.pause(partitions)

    try:
        backoff_until = time.time() + settings.keep_backoff_seconds
        while time.time() < backoff_until and not shutdown_event.is_set():
            _poll_paused(consumer)
            shutdown_event.wait(timeout=settings.keep_poll_interval_seconds)
    finally:
        if partitions:
            consumer.resume(partitions)
        if shutdown_event.is_set():
            logger.info("Backoff interrupted by shutdown signal")
        else:
            logger.info("Backoff complete; resuming processing")


def _record_position(msg) -> dict:
    return {"topic": msg.topic(), "partition": msg.partition(), "offset": msg.offset()}


def _is_alert_or_batch(alert: Any) -> bool:
    if isinstance(alert, dict):
        return True
    return isinstance(alert, list) and bool(alert) and all(
        isinstance(item, dict) for item in alert
    )


def _extract_alert(payload: dict) -> tuple[dict | list[dict], dict, str | None]:
    # Two DLQ payload shapes reach this consumer:
    #  - keep-ingestion contract: {"alert": {...}, "error": {...}}
    #  - keep-api-gateway fallback: {"event": {...}, "event_type": "alert", ...}
    # Falling back to the raw payload keeps best-effort support for bare alerts.
    alert = payload.get("alert") or payload.get("event") or payload
    error = payload.get("error") or {}
    fingerprint = fingerprint_of(alert) if _is_alert_or_batch(alert) else None
    return alert, error, fingerprint


def _replay_count(payload: dict) -> int:
    try:
        return int(payload.get("dlq_replay_count") or 0)
    except (TypeError, ValueError):
        return 0


def _wait_for_replay(
    consumer: Consumer,
    msg,
    replay_count: int,
    shutdown_event: threading.Event,
    log_context: dict | None = None,
) -> bool:
    """Hold the alert until it has sat in the DLQ for the replay's delay.

    The delay doubles per replay up to KEEP_MAX_REPLAY_DELAY_SECONDS, so an
    alert keep-event-handler keeps failing (e.g. a DB outage) is spaced out
    instead of looping straight back. Returns
    False if shutdown interrupted the wait; the alert is then left uncommitted.
    """
    timestamp_type, timestamp_ms = msg.timestamp()
    if timestamp_type == TIMESTAMP_NOT_AVAILABLE or timestamp_ms <= 0:
        return True
    delay = min(
        settings.keep_replay_delay_seconds * 2**replay_count,
        settings.keep_max_replay_delay_seconds,
    )
    ready_at = timestamp_ms / 1000 + delay
    if time.time() >= ready_at:
        return True

    logger.info(
        "Delaying DLQ replay",
        extra={
            **(log_context or {}),
            "replay": replay_count + 1,
            "delay_seconds": round(ready_at - time.time()),
        },
    )
    partitions = consumer.assignment() or []
    if partitions:
        consumer.pause(partitions)
    try:
        while time.time() < ready_at and not shutdown_event.is_set():
            _poll_paused(consumer)
    finally:
        if partitions:
            consumer.resume(partitions)
    return not shutdown_event.is_set()


def consume(shutdown_event: threading.Event, consumer_state: ConsumerState):
    with KeepClient() as keep_client:
        consumer = Consumer(kafka_config())
        # Being assigned partitions means the consumer is back in the group, so
        # /health recovers after a crash even while the DLQ is empty.
        consumer.subscribe(
            [settings.kafka_dlq_topic],
            on_assign=lambda _consumer, _partitions: consumer_state.mark_healthy(),
        )
        logger.info(
            "Kafka consumer started",
            extra={
                "topic": settings.kafka_dlq_topic,
                "group_id": settings.kafka_dlq_group_id,
            },
        )
        consecutive_failures = 0

        try:
            while not shutdown_event.is_set():
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise KafkaException(msg.error())

                try:
                    payload = json.loads(msg.value().decode("utf-8"))
                except Exception:
                    logger.exception(
                        "Failed to decode DLQ message; skipping offset",
                        extra=_record_position(msg),
                    )
                    commit(consumer, msg)
                    consumer_state.record_decode_error()
                    continue

                if not isinstance(payload, dict):
                    logger.error(
                        "DLQ message payload is not a JSON object; skipping offset",
                        extra={
                            **_record_position(msg),
                            "payload_type": type(payload).__name__,
                        },
                    )
                    commit(consumer, msg)
                    consumer_state.record_decode_error()
                    continue

                alert, error, fingerprint = _extract_alert(payload)
                reason = error.get("reason") if isinstance(error, dict) else None
                replay_count = _replay_count(payload)
                log_context = {
                    "fingerprint": fingerprint,
                    "trace_id": payload.get("trace_id"),
                    "tenant_id": payload.get("tenant_id"),
                }
                if isinstance(alert, list):
                    log_context["alerts_count"] = len(alert)

                logger.info(
                    "DLQ alert received",
                    extra={**log_context, "reason": reason, "replay": replay_count},
                )

                if not _is_alert_or_batch(alert):
                    logger.error(
                        "DLQ alert field is not an object or a list of objects; skipping offset",
                        extra={
                            **_record_position(msg),
                            **log_context,
                            "payload_type": type(alert).__name__,
                        },
                    )
                    commit(consumer, msg)
                    consumer_state.record_decode_error()
                    continue

                if reason in UNREPLAYABLE_REASONS:
                    logger.error(
                        "Not replaying DLQ alert that failed as poison or malformed",
                        extra={
                            **log_context,
                            "reason": reason,
                            "dropped_alert": alert,
                        },
                    )
                    commit(consumer, msg)
                    consumer_state.record_dropped()
                    continue

                if replay_count >= settings.keep_max_dlq_replays:
                    logger.error(
                        "Dropping DLQ alert after max replays",
                        extra={
                            **log_context,
                            "replays": replay_count,
                            "dropped_alert": alert,
                        },
                    )
                    commit(consumer, msg)
                    consumer_state.record_dropped()
                    continue

                if not _wait_for_replay(
                    consumer, msg, replay_count, shutdown_event, log_context
                ):
                    break

                result = keep_client.send(alert, replay=replay_count + 1)
                if result == DeliveryResult.SUCCESS:
                    logger.info(
                        "DLQ alert re-ingested",
                        extra={
                            **log_context,
                            "replay": replay_count + 1,
                            "result": result.value,
                        },
                    )
                    consecutive_failures = 0
                    consumer_state.record_success()
                    commit(consumer, msg)
                elif result == DeliveryResult.TERMINAL:
                    logger.error(
                        "Failed to re-ingest DLQ alert",
                        extra={**log_context, "result": result.value},
                    )
                    consecutive_failures += 1
                    consumer_state.record_terminal()
                    commit(consumer, msg)
                else:
                    logger.warning(
                        "Failed to re-ingest DLQ alert; will retry",
                        extra={**log_context, "result": result.value},
                    )
                    consecutive_failures += 1
                    consumer_state.record_retryable_exhausted()

                    # Skipping the commit alone isn't enough: poll() would move on
                    # and the next commit would cover this offset too. Rewind so
                    # the same alert is retried until Keep is back. Retryable
                    # means Keep is unavailable, so later alerts would fail too.
                    consumer.seek(
                        TopicPartition(msg.topic(), msg.partition(), msg.offset())
                    )

                if consecutive_failures >= settings.keep_max_consecutive_failures:
                    consumer_state.record_backoff()
                    logger.error(
                        "Entering backoff after consecutive failures",
                        extra={
                            "consecutive_failures": consecutive_failures,
                            "backoff_seconds": settings.keep_backoff_seconds,
                        },
                    )
                    backoff(consumer, shutdown_event)
                    consumer_state.record_backoff_end()
                    consecutive_failures = 0
        finally:
            consumer.close()
            logger.info("Kafka consumer stopped")
