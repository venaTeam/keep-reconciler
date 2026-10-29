import http.client
import inspect
import json
import logging
import logging.config
import logging.handlers
import os
import queue
import sys
import threading

import requests
import urllib3
from pythonjsonlogger import jsonlogger

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger(__name__)

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
KEEP_LOG_FILE = os.environ.get("KEEP_LOG_FILE", "false").lower() == "true"
KEEP_FLUENTBIT = os.environ.get("KEEP_FLUENTBIT", "true").lower() == "true"
KEEP_FLUENTBIT_HOST = os.environ.get("KEEP_FLUENTBIT_HOST")
KEEP_FLUENTBIT_PORT = os.environ.get("KEEP_FLUENTBIT_PORT", "80")
RUNNING_IN_CLOUD_RUN = os.environ.get("K_SERVICE") is not None

LOG_FORMAT_OPEN_TELEMETRY = "open_telemetry"
LOG_FORMAT_DEVELOPMENT_TERMINAL = "dev_terminal"
LOG_FORMAT = os.environ.get("LOG_FORMAT", LOG_FORMAT_OPEN_TELEMETRY)
OTEL_SERVICE_NAME = os.environ.get("OTEL_SERVICE_NAME", "keep-reconciler")


class OtelFieldsFilter(logging.Filter):
    """Ensure OTEL correlation fields exist on every log record.

    Without a real OpenTelemetry SDK setup, formatters that reference
    `%(otelServiceName)s` would emit empty values. This filter sets a
    default service name from `OTEL_SERVICE_NAME` so FluentBit can route
    reconciler logs by `$otelServiceName`.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, "otelTraceID"):
            record.otelTraceID = "-"
        if not hasattr(record, "otelSpanID"):
            record.otelSpanID = "-"
        if not hasattr(record, "otelTraceSampled"):
            record.otelTraceSampled = "-"
        if not hasattr(record, "otelServiceName"):
            record.otelServiceName = OTEL_SERVICE_NAME
        return True


def get_gunicorn_log_level():
    """
    Check for --log-level flag in gunicorn/uvicorn command line arguments.
    Returns the log level or Keep's log level if not found.
    """
    log_level = None
    try:
        for i, arg in enumerate(sys.argv):
            if arg == "--log-level" and i + 1 < len(sys.argv):
                log_level = sys.argv[i + 1].upper()
                break
            elif arg.startswith("--log-level="):
                log_level = arg.split("=", 1)[1].upper()
                break
    except Exception:
        pass

    valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
    if log_level in valid_levels:
        return log_level

    return LOG_LEVEL


class DevTerminalFormatter(logging.Formatter):
    def format(self, record):
        if not hasattr(record, "otelTraceID"):
            record.otelTraceID = "-"

        message = super().format(record)
        extra_info = ""

        frame = inspect.currentframe()
        while frame:
            if frame.f_code.co_name == "_log":
                extra = frame.f_locals.get("extra", {})
                if extra:
                    extra_info = " ".join(
                        [f"[{k}: {v}]" for k, v in extra.items() if k != "raw_event"]
                    )
                break
            frame = frame.f_back

        return f"{message} {extra_info}"


class FluentBitHandler(logging.Handler):
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    def __init__(self, host, port, tenant="keep", **kwargs):
        super().__init__()
        self.url = f"http://{host}:{port}"
        self.tenant = tenant
        self.queue = queue.Queue(maxsize=1000)
        self.session = requests.Session()
        self._stop = threading.Event()
        self._send_thread = None
        self._start_sender()

    def _start_sender(self):
        self._send_thread = threading.Thread(target=self._send, daemon=True)
        self._send_thread.start()

    def _send(self):
        # The try must cover everything: any uncaught exception here
        # would kill the sender thread and silently drop every future record.
        while not self._stop.is_set():
            try:
                try:
                    record = self.queue.get(timeout=1)
                except queue.Empty:
                    continue
                if record is None:
                    break
                json_record = json.loads(record)
                json_record["tenant"] = self.tenant
                try:
                    self.session.post(self.url, json=json_record, verify=False)
                except Exception:
                    pass
            except Exception:
                # Malformed record or unexpected error: skip it, keep sending.
                continue

    def emit(self, record):
        # dictConfig-driven reconfiguration elsewhere in the process (e.g.
        # a library calling logging.config) calls close() on this handler,
        # killing the sender thread. Records would then pile up in the queue
        # forever and be silently dropped. Resurrect the sender thread.
        if self._stop.is_set() and not self._send_thread.is_alive():
            self._stop.clear()
            self._start_sender()
        try:
            self.queue.put_nowait(self.format(record))
        except queue.Full:
            pass

    def close(self):
        try:
            self.queue.put(None)
            self._stop.set()
        except Exception:
            pass
        super().close()


class CustomJsonFormatter(jsonlogger.JsonFormatter):
    def __init__(self, *args, rename_fields=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.rename_fields = rename_fields if RUNNING_IN_CLOUD_RUN else {}

    def add_fields(self, log_record, record, message_dict):
        super().add_fields(log_record, record, message_dict)


class UvicornAccessFormatter(logging.Formatter):
    def format(self, record):
        if not hasattr(record, "otelTraceID"):
            record.otelTraceID = "-"
        return super().format(record)


CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "json": {
            "()": CustomJsonFormatter,
            "fmt": "%(asctime)s %(message)s %(levelname)s %(name)s %(filename)s %(otelTraceID)s %(otelSpanID)s %(otelTraceSampled)s %(otelServiceName)s %(threadName)s %(process)s %(module)s",
            "rename_fields": {
                "levelname": "severity",
                "asctime": "timestamp",
                "otelTraceID": "logging.googleapis.com/trace",
                "otelSpanID": "logging.googleapis.com/spanId",
                "otelTraceSampled": "logging.googleapis.com/trace_sampled",
            },
        },
        "dev_terminal": {
            "()": DevTerminalFormatter,
            "format": "%(asctime)s - %(otelTraceID)s %(threadName)s %(levelname)s - %(message)s",
        },
        "uvicorn_access": {
            "()": UvicornAccessFormatter,
            "format": "%(asctime)s - %(otelTraceID)s - %(threadName)s - %(message)s",
        },
    },
    "handlers": {
        "default": {
            "level": LOG_LEVEL,
            "formatter": (
                "json" if LOG_FORMAT == LOG_FORMAT_OPEN_TELEMETRY else "dev_terminal"
            ),
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stdout",
            "filters": ["otel_fields"],
        },
        "uvicorn_access": {
            "class": "logging.StreamHandler",
            "formatter": "uvicorn_access",
            "stream": "ext://sys.stdout",
            "filters": ["otel_fields"],
        },
    },
    "filters": {
        "otel_fields": {"()": OtelFieldsFilter},
    },
    "loggers": {
        "": {
            "handlers": ["default"],
            "level": LOG_LEVEL,
            "propagate": False,
        },
        "uvicorn.access": {
            "()": "src.logging.CustomizedUvicornLogger",
            "handlers": ["uvicorn_access"],
            "level": get_gunicorn_log_level(),
            "propagate": False,
        },
        "uvicorn.error": {
            "()": "src.logging.CustomizedUvicornLogger",
            "handlers": ["default"],
            "level": get_gunicorn_log_level(),
            "propagate": False,
        },
        "opentelemetry.context": {
            "handlers": [],
            "level": "CRITICAL",
            "propagate": False,
        },
        "httpx": {
            "handlers": [],
            "level": "ERROR",
            "propagate": False,
        },
        "http.client": {
            "handlers": ["default"],
            "level": "DEBUG",
            "propagate": False,
        },
    },
}


class CustomizedUvicornLogger(logging.Logger):
    """Overrides the default Uvicorn logger to propagate trace/tenant context."""

    def makeRecord(
        self,
        name,
        level,
        fn,
        lno,
        msg,
        args,
        exc_info,
        func=None,
        extra=None,
        sinfo=None,
    ):
        trace_id = extra.get("otelTraceID") if extra else None
        rv = super().makeRecord(
            name, level, fn, lno, msg, args, exc_info, func, extra, sinfo
        )
        rv.__dict__["otelTraceID"] = trace_id if trace_id else "-"
        return rv

    def _log(
        self,
        level,
        msg,
        args,
        exc_info=None,
        extra=None,
        stack_info=False,
        stacklevel=1,
    ):
        frame = inspect.currentframe().f_back
        while frame:
            found_frame = False
            if frame.f_code.co_name == "run_asgi":
                trace_id = (
                    frame.f_locals.get("self")
                    .scope.get("state", {})
                    .get("trace_id", "-")
                )
                tenant_id = (
                    frame.f_locals.get("self")
                    .scope.get("state", {})
                    .get("tenant_id", 0)
                )
                if extra is None:
                    extra = {}
                extra.update({"otelTraceID": trace_id})
                found_frame = True
                if tenant_id:
                    extra.update({"tenant_id": tenant_id})
                    found_frame = True

            if found_frame:
                break
            frame = frame.f_back

        logging.Logger._log(
            self, level, msg, args, exc_info, extra, stack_info, stacklevel
        )


def setup_logging():
    if KEEP_LOG_FILE:
        CONFIG["handlers"]["file"] = {
            "level": "DEBUG",
            "formatter": "json",
            "class": "logging.handlers.RotatingFileHandler",
            "filename": KEEP_LOG_FILE,
            "mode": "a",
            "maxBytes": 1024 * 1024 * 1024,
            "backupCount": 5,
        }
        CONFIG["loggers"][""]["handlers"].append("file")

    if KEEP_FLUENTBIT:
        CONFIG["handlers"]["fluentbit"] = {
            "level": "DEBUG",
            "formatter": "json",
            "class": "src.logging.FluentBitHandler",
            "host": KEEP_FLUENTBIT_HOST,
            "port": int(KEEP_FLUENTBIT_PORT),
            "filters": ["otel_fields"],
        }
        CONFIG["loggers"][""]["handlers"].append("fluentbit")

    logging.config.dictConfig(CONFIG)

    http_client_logger = logging.getLogger("http.client")
    http_client_logger.setLevel(logging.DEBUG)
    http.client.HTTPConnection.debuglevel = 1

    def print_to_log(*args):
        http_client_logger.debug(" ".join(args))

    http.client.print = print_to_log
