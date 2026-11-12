import os

# src.config builds Settings() at import time, so the required env vars must be
# set before any src module is imported.
os.environ.setdefault("KEEP_URL", "http://keep.test")
os.environ.setdefault("KEEP_API_KEY", "test-key")
os.environ.setdefault("KAFKA_DLQ_BOOTSTRAP_SERVERS", "localhost:9092")
os.environ.setdefault("KAFKA_DLQ_SASL_USERNAME", "user")
os.environ.setdefault("KAFKA_DLQ_SASL_PASSWORD", "password")
os.environ.setdefault("KAFKA_DLQ_TOPIC", "keep-dlq")
os.environ.setdefault("KEEP_FLUENTBIT", "false")
os.environ.setdefault("LOG_FORMAT", "dev_terminal")
