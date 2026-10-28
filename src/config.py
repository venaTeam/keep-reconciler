import json

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        case_sensitive=False,
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    keep_url: str
    keep_api_key: str
    port: int = 8087
    keep_send_retries: int = 3
    keep_timeout_seconds: int = 10
    keep_max_consecutive_failures: int = 10
    keep_backoff_seconds: int = 600
    keep_poll_interval_seconds: int = 180
    keep_consumer_restart_limit: int = 5
    keep_consumer_restart_window: int = 300
    keep_commit_retries: int = 3

    kafka_dlq_bootstrap_servers: str
    kafka_dlq_group_id: str = "keep-dlq-reingestion"
    kafka_dlq_sasl_username: str
    kafka_dlq_sasl_password: str
    kafka_dlq_topic: str

    @property
    def kafka_dlq_bootstrap_servers_list(self) -> list[str]:
        value = self.kafka_dlq_bootstrap_servers.strip()
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return [server.strip() for server in value.split(",") if server.strip()]
        if isinstance(parsed, list):
            return [str(item) for item in parsed]
        return [str(parsed)]

    @property
    def keep_alerts_event_url(self) -> str:
        return f"{self.keep_url}/alerts/event"


settings = Settings()
