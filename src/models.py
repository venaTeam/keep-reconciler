from enum import Enum

from pydantic import BaseModel


class DeliveryResult(str, Enum):
    SUCCESS = "success"
    TERMINAL = "terminal"
    RETRYABLE_EXHAUSTED = "retryable_exhausted"


class DlqMessage(BaseModel):
    alert: dict
    error: dict | None = None
