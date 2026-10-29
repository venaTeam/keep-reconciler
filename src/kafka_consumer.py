import json
import logging
import threading
import time
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition

from src.config import settings
from src.keep_client import KeepClient
from src.models import DeliveryResult
from src.state import ConsumerState

logger = logging.getLogger("keep-reconciler")


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
            logger.error(
                "Kafka commit attempt %d/%d failed: %s",
                attempt + 1,
                settings.keep_commit_retries,
                e,
            )
            if attempt < settings.keep_commit_retries - 1:
                time.sleep(2**attempt)
    logger.error(
        "Kafka commit failed after %d attempts; raising to restart consumer: %s(%s)",
        settings.keep_commit_retries,
        type(last_error).__name__ if last_error else "unknown",
        last_error if last_error else "no error captured",
    )
    raise last_error if last_error else RuntimeError("Kafka commit failed")


def backoff(consumer: Consumer, shutdown_event: threading.Event) -> None:
    logger.error(
        "Entering backoff: pausing processing for %d seconds",
        settings.keep_backoff_seconds,
    )
    partitions = consumer.assignment() or []
    if partitions:
        consumer.pause(partitions)

    try:
        backoff_until = time.time() + settings.keep_backoff_seconds
        while time.time() < backoff_until and not shutdown_event.is_set():
            # poll() keeps the consumer heartbeating while paused.
            consumer.poll(timeout=1.0)
            shutdown_event.wait(timeout=settings.keep_poll_interval_seconds)
    finally:
        if partitions:
            consumer.resume(partitions)
        if shutdown_event.is_set():
            logger.info("Backoff interrupted by shutdown signal")
        else:
            logger.info("Backoff complete; resuming processing")


def _extract_alert(payload: dict) -> tuple[dict, dict, str | None]:
    # Two DLQ payload shapes reach this consumer:
    #  - keep-ingestion contract: {"alert": {...}, "error": {...}}
    #  - keep-api-gateway fallback: {"event": {...}, "event_type": "alert", ...}
    # Falling back to the raw payload keeps best-effort support for bare alerts.
    alert = payload.get("alert") or payload.get("event") or payload
    error = payload.get("error") or {}
    fingerprint = alert.get("fingerprint") if isinstance(alert, dict) else None
    return alert, error, fingerprint


def consume(shutdown_event: threading.Event, consumer_state: ConsumerState):
    with KeepClient() as keep_client:
        consumer = Consumer(kafka_config())
        consumer.subscribe([settings.kafka_dlq_topic])
        logger.info("Kafka consumer started")
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
                    logger.exception("Failed to decode DLQ message; skipping offset")
                    commit(consumer, msg)
                    consumer_state.record_decode_error()
                    continue

                if not isinstance(payload, dict):
                    logger.error(
                        "DLQ message payload is not a JSON object; skipping offset: %s",
                        type(payload).__name__,
                    )
                    commit(consumer, msg)
                    consumer_state.record_decode_error()
                    continue

                alert, error, fingerprint = _extract_alert(payload)

                logger.info(
                    "DLQ alert received from alerts-gateway: fingerprint=%s error=%s",
                    fingerprint,
                    error,
                    extra={
                        "fingerprint": fingerprint,
                        "error": error,
                    },
                )

                if not isinstance(alert, dict):
                    logger.error(
                        "DLQ alert field is not an object; skipping offset: %s",
                        type(alert).__name__,
                    )
                    commit(consumer, msg)
                    consumer_state.record_decode_error()
                    continue

                result = keep_client.send(alert)
                if result == DeliveryResult.SUCCESS:
                    logger.info(
                        "Successfully re-ingested DLQ alert: fingerprint=%s",
                        alert.get("fingerprint"),
                        extra={
                            "fingerprint": alert.get("fingerprint"),
                            "result": result.value,
                        },
                    )
                    consecutive_failures = 0
                    consumer_state.record_success()
                    commit(consumer, msg)
                elif result == DeliveryResult.TERMINAL:
                    logger.error(
                        "Failed to re-ingest DLQ alert (result=%s): fingerprint=%s",
                        result.value,
                        alert.get("fingerprint"),
                        extra={
                            "fingerprint": alert.get("fingerprint"),
                            "result": result.value,
                        },
                    )
                    consecutive_failures += 1
                    consumer_state.record_terminal()
                    commit(consumer, msg)
                else:
                    logger.error(
                        "Failed to re-ingest DLQ alert (result=%s): fingerprint=%s",
                        result.value,
                        alert.get("fingerprint"),
                        extra={
                            "fingerprint": alert.get("fingerprint"),
                            "result": result.value,
                        },
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
                        "Entering backoff after %d consecutive failures",
                        consecutive_failures,
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
