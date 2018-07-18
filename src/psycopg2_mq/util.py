from datetime import datetime, timedelta

EPOCH = datetime(1970, 1, 1)


def datetime_to_int(value):
    return int((value - EPOCH).total_seconds())


def int_to_datetime(value):
    return EPOCH + timedelta(seconds=value)


def clamp(v, lo, hi):
    return min(max(v, lo), hi)
