import time
import os


class CircuitBreakerOpen(Exception):
    pass


class CircuitBreaker:
    def __init__(
        self,
        failure_threshold=None,
        recovery_timeout=None
    ):
        self.failure_threshold = failure_threshold or int(
            os.getenv("CB_FAILURE_THRESHOLD", 3)
        )
        self.recovery_timeout = recovery_timeout or int(
            os.getenv("CB_RECOVERY_TIMEOUT", 60)
        )

        self.failure_count = 0
        self.last_failure_time = None
        self.state = "CLOSED"

    # -----------------------------
    #   Stato del circuito
    # -----------------------------
    def before_call(self):
        print(f"[CB] Stato prima chiamata: {self.state}", flush=True)
        if self.state == "OPEN":
            elapsed = time.time() - self.last_failure_time
            if elapsed >= self.recovery_timeout:
                print("[CB] Passaggio a HALF_OPEN", flush=True)
                self.state = "HALF_OPEN"
            else:
                raise CircuitBreakerOpen("Circuit breaker OPEN")

    def on_success(self):
        print("[CB] Successo → reset circuito", flush=True)
        self.failure_count = 0
        self.state = "CLOSED"

    def on_failure(self):
        self.failure_count += 1
        self.last_failure_time = time.time()
        print(f"[CB] Failure #{self.failure_count}", flush=True)

        if self.failure_count >= self.failure_threshold:
            print("[CB] Circuito OPEN", flush=True)
            self.state = "OPEN"

