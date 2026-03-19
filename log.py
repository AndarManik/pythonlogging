import contextvars
import logging
import secrets
import string

_BASE62 = string.digits + string.ascii_lowercase + string.ascii_uppercase
_last_log_id = contextvars.ContextVar("last_log_id", default=None)
_factory_installed = False


def new_id(n=8):
    return "".join(secrets.choice(_BASE62) for _ in range(n))


def causal_wrap(fn):
    parent = _last_log_id.get()

    def wrapped(*args, **kwargs):
        token = _last_log_id.set(parent)
        try:
            return fn(*args, **kwargs)
        finally:
            _last_log_id.reset(token)

    return wrapped


def install_log_record_factory():
    global _factory_installed
    if _factory_installed:
        return

    old_factory = logging.getLogRecordFactory()

    def record_factory(*args, **kwargs):
        record = old_factory(*args, **kwargs)

        parent_id = _last_log_id.get()
        log_id = new_id()

        record.parent_id = parent_id
        record.log_id = log_id

        # backward-compatible aliases
        record.parent = parent_id
        record.id = log_id

        _last_log_id.set(log_id)
        return record

    logging.setLogRecordFactory(record_factory)
    _factory_installed = True


def set_log_root(value=None):
    return _last_log_id.set(value)


def reset_log_root(token):
    _last_log_id.reset(token)


class LogFilter(logging.Filter):
    def filter(self, record):
        record.parent_id = getattr(record, "parent_id", None)
        record.log_id = getattr(record, "log_id", None)

        record.parent = getattr(record, "parent", record.parent_id)
        record.id = getattr(record, "id", record.log_id)

        record.modules = getattr(record, "modules", record.module)
        record.task = getattr(record, "task", "-")
        record.ci = getattr(record, "ci", "-")
        return True