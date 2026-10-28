import threading


class ConsumerState:
    def __init__(self):
        self._lock = threading.Lock()
        self.healthy = True
        self.in_backoff = False
        self.counters = {
            "messages_total": 0,
            "success_total": 0,
            "terminal_total": 0,
            "retryable_exhausted_total": 0,
            "decode_errors_total": 0,
            "backoffs_total": 0,
        }

    def record_success(self):
        with self._lock:
            self.counters["messages_total"] += 1
            self.counters["success_total"] += 1
            self.healthy = True
            self.in_backoff = False

    def record_terminal(self):
        with self._lock:
            self.counters["messages_total"] += 1
            self.counters["terminal_total"] += 1

    def record_retryable_exhausted(self):
        with self._lock:
            self.counters["messages_total"] += 1
            self.counters["retryable_exhausted_total"] += 1

    def record_decode_error(self):
        with self._lock:
            self.counters["decode_errors_total"] += 1

    def record_backoff(self):
        with self._lock:
            self.in_backoff = True
            self.counters["backoffs_total"] += 1

    def record_backoff_end(self):
        with self._lock:
            self.in_backoff = False

    def mark_unhealthy(self):
        with self._lock:
            self.healthy = False

    def get_counters(self):
        with self._lock:
            return dict(self.counters)

    def is_healthy(self):
        with self._lock:
            return self.healthy

    def is_in_backoff(self):
        with self._lock:
            return self.in_backoff
