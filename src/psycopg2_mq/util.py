import builtins
from collections.abc import Mapping
from datetime import datetime, timedelta
from dateutil.rrule import rrulestr
from dateutil.tz import UTC

EPOCH = datetime(1970, 1, 1)


def datetime_to_int(value):
    return int((value - EPOCH).total_seconds())


def int_to_datetime(value):
    return EPOCH + timedelta(seconds=value)


def class_name(cls):
    name = cls.__qualname__
    module = cls.__module__
    if module is not None and module != builtins.__name__:
        name = module + '.' + name
    return name


# cribbed from sentry_sdk
def safe_object(obj, *, memo=None):
    if memo is None:
        memo = Memo()
    if memo.memoize(obj):
        return '<cycle type={}>'.format(type(obj).__name__)

    try:
        if isinstance(obj, (tuple, list)):
            return [
                safe_object(x, memo=memo)
                for x in obj
            ]

        if isinstance(obj, Mapping):
            return {
                safe_str(k): safe_object(v, memo=memo)
                for k, v in list(obj.items())
            }

        return safe_repr(obj)

    finally:
        memo.unmemoize(obj)

    return '<broken repr>'


def safe_str(value):
    try:
        return str(value)
    except Exception:
        return safe_repr(value)


def safe_repr(value):
    try:
        if isinstance(value, bytes):
            value = value.decode('latin1', 'replace')
        return repr(value)
    except Exception:
        return '<broken repr>'


class Memo:
    def __init__(self):
        self._inner = {}

    def memoize(self, obj):
        if id(obj) in self._inner:
            return True
        self._inner[id(obj)] = obj
        return False

    def unmemoize(self, obj):
        self._inner.pop(id(obj), None)


def get_next_rrule_time(rrule, dtstart, after):
    rrule = rrulestr(rrule, dtstart=dtstart)
    try:
        ts = rrule.after(after)
    except Exception:
        # we do not know if the rrule's dtstart is timezone-aware or not
        # and dateutil doesn't allow us to provide a default of UTC and
        # so what we do is try again with a tz-aware object
        after = after.replace(tzinfo=UTC)
        ts = rrule.after(after)

    if ts is not None and ts.tzinfo is not None:
        ts = ts.astimezone(UTC).replace(tzinfo=None)
    return ts
