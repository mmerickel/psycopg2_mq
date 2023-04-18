from dateutil.parser import parse as parse_datetime


def test_rrule():
    from psycopg2_mq.util import get_next_rrule_time

    now = parse_datetime('2023-04-01T06:00:00Z')
    target = get_next_rrule_time('FREQ=MONTHLY;BYDAY=1MO', now, after=now)
    assert target.isoformat() == '2023-04-03T06:00:00'
