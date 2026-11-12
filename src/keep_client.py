import logging
import time
from typing import Any

import httpx
import starlette.status as status

from src.config import settings
from src.models import DeliveryResult

logger = logging.getLogger("keep-reconciler")

# Tells keep-ingestion this is a replay: it must not divert it back into the
# DLQ, and it carries the count on so the next DLQ write keeps it.
REPLAY_HEADER = "X-Keep-DLQ-Replay"


def _preserve_last_received(alert: dict[str, Any]) -> dict[str, Any]:
    """Ensure a replayed alert carries a stable `lastReceived`.

    The generic Keep intake endpoint defaults a missing `lastReceived` to the
    current wall-clock time. For DLQ replays that means every re-ingestion of
    the same alert becomes a "new" duplicate with a fresh `last_received`, so
    `lastAlert.last_received` (and the UI feed) advances on every replay even
    though the underlying event has not changed.

    When the original alert omitted `lastReceived`, fall back to the event's
    own timestamp fields so the replay stays stable and idempotent.
    """
    if alert.get("lastReceived") or alert.get("last_received"):
        # Normalise to the alias the generic endpoint expects, in case the
        # stored payload used the snake_case field name.
        if not alert.get("lastReceived") and alert.get("last_received"):
            alert = {**alert, "lastReceived": alert["last_received"]}
        return alert

    for source_key in ("timestamp", "timeCreated", "startedAt", "firingStartTime"):
        value = alert.get(source_key)
        if value:
            return {**alert, "lastReceived": value}

    logger.warning(
        "DLQ alert has no lastReceived/timestamp fallback; Keep will default to now",
        extra={"fingerprint": alert.get("fingerprint")},
    )
    return alert


def fingerprint_of(alert: dict | list[dict]) -> str | None:
    """The alert's fingerprint for logs; comma-joined for a batch."""
    if isinstance(alert, list):
        return ",".join(str(a.get("fingerprint")) for a in alert) or None
    return alert.get("fingerprint")


class KeepClient:
    def __init__(self, timeout: int | None = None):
        self.timeout = timeout or settings.keep_timeout_seconds
        self._client = httpx.Client(timeout=self.timeout)

    def send(self, alert: dict | list[dict], replay: int = 1) -> DeliveryResult:
        # /alerts/event accepts a batch too, so a list is resent as one request.
        if isinstance(alert, list):
            alert = [_preserve_last_received(item) for item in alert]
        else:
            alert = _preserve_last_received(alert)
        log_context = {"fingerprint": fingerprint_of(alert)}
        if isinstance(alert, list):
            log_context["alerts_count"] = len(alert)
        last_error: Exception | None = None
        for attempt in range(settings.keep_send_retries):
            try:
                response = self._client.post(
                    settings.keep_alerts_event_url,
                    json=alert,
                    headers={
                        "X-API-KEY": settings.keep_api_key,
                        REPLAY_HEADER: str(replay),
                    },
                )
                if response.is_success:
                    return DeliveryResult.SUCCESS
                if (
                    response.is_client_error
                    and response.status_code != status.HTTP_429_TOO_MANY_REQUESTS
                ):
                    logger.error(
                        "Keep rejected alert with terminal status",
                        extra={
                            **log_context,
                            "status_code": response.status_code,
                            "response_body": response.text[:500],
                        },
                    )
                    return DeliveryResult.TERMINAL
                last_error = Exception(
                    f"HTTP {response.status_code}: {response.text[:500]}"
                )
                logger.warning(
                    "Keep returned retryable status",
                    extra={
                        **log_context,
                        "status_code": response.status_code,
                        "attempt": attempt + 1,
                        "max_attempts": settings.keep_send_retries,
                    },
                )
            except Exception as e:
                last_error = e
                logger.warning(
                    "Keep delivery attempt failed",
                    extra={
                        **log_context,
                        "attempt": attempt + 1,
                        "max_attempts": settings.keep_send_retries,
                        "error": str(e),
                    },
                )
            time.sleep(2**attempt)

        logger.error(
            "Keep delivery exhausted all retries",
            extra={
                **log_context,
                "retries": settings.keep_send_retries,
                "error_type": type(last_error).__name__ if last_error else "unknown",
                "error": str(last_error) if last_error else "no error captured",
            },
        )
        return DeliveryResult.RETRYABLE_EXHAUSTED

    def close(self):
        self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False
