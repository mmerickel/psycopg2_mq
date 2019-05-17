from collections.abc import Mapping
from datetime import datetime, timedelta

EPOCH = datetime(1970, 1, 1)


def datetime_to_int(value):
    return int((value - EPOCH).total_seconds())


def int_to_datetime(value):
    return EPOCH + timedelta(seconds=value)


def clamp(v, lo, hi):
    return min(max(v, lo), hi)


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
        rv = repr(value)
        if isinstance(rv, bytes):
            rv = rv.decode('utf-8', 'replace')

        # At this point `rv` contains a bunch of literal escape codes, like
        # this (exaggerated example):
        #
        # u"\\x2f"
        #
        # But we want to show this string as:
        #
        # u"/"
        try:
            # unicode-escape does this job, but can only decode latin1. So we
            # attempt to encode in latin1.
            return rv.encode("latin1").decode("unicode-escape")
        except Exception:
            # Since usually strings aren't latin1 this can break. In those
            # cases we just give up.
            return rv
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
