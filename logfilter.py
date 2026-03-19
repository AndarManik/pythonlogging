import contextvars
import logging
import secrets
import string
import time
from concurrent.futures import ThreadPoolExecutor, wait

# =========================
# Core causal logging setup
# =========================

_BASE62 = string.digits + string.ascii_lowercase + string.ascii_uppercase
_last_log_id = contextvars.ContextVar("last_log_id", default=None)


def new_id(n=8):
    return "".join(secrets.choice(_BASE62) for _ in range(n))


old_factory = logging.getLogRecordFactory()


def record_factory(*args, **kwargs):
    record = old_factory(*args, **kwargs)

    parent_id = _last_log_id.get()
    log_id = new_id()

    record.parent_id = parent_id
    record.log_id = log_id

    # advance chain in this context
    _last_log_id.set(log_id)
    return record


logging.setLogRecordFactory(record_factory)


# =========================
# Logging config
# =========================

def configure_logging():
    root = logging.getLogger()
    root.setLevel(logging.INFO)

    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(
        "%(asctime)s %(levelname)s "
        "%(log_id)s %(parent_id)s %(message)s"
    ))

    root.handlers.clear()
    root.addHandler(handler)


log = logging.getLogger("demo")


# =========================
# Worker function
# =========================

def worker(n):
    log.info(f"[worker {n}] start")
    time.sleep(0.05)
    log.info(f"[worker {n}] end")


# =========================
# Helper: propagate context
# =========================

def causal_wrap(fn):
    parent = _last_log_id.get()

    def wrapped(*args, **kwargs):
        token = _last_log_id.set(parent)
        try:
            return fn(*args, **kwargs)
        finally:
            _last_log_id.reset(token)

    return wrapped


# =========================
# Demo scenarios
# =========================

def serial_demo():
    log.info("=== SERIAL DEMO START ===")

    log.info("step A")
    log.info("step B")
    log.info("step C")

    log.info("=== SERIAL DEMO END ===")


def threadpool_no_propagation():
    log.info("=== THREADPOOL (NO PROPAGATION) START ===")

    log.info("before submit")

    with ThreadPoolExecutor(max_workers=3) as pool:
        futures = [pool.submit(worker, i) for i in range(3)]
        wait(futures)

    log.info("after join")

    log.info("=== THREADPOOL (NO PROPAGATION) END ===")


def threadpool_with_propagation():
    log.info("=== THREADPOOL (WITH PROPAGATION) START ===")

    log.info("before submit")

    with ThreadPoolExecutor(max_workers=3) as pool:
        futures = [pool.submit(causal_wrap(worker), i) for i in range(3)]
        wait(futures)

    log.info("after join")

    log.info("=== THREADPOOL (WITH PROPAGATION) END ===")


# =========================
# Main
# =========================

if __name__ == "__main__":
    configure_logging()

    serial_demo()
    print("\n")

    threadpool_no_propagation()
    print("\n")

    threadpool_with_propagation()