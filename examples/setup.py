import argparse
from datetime import timedelta
import logging
import sqlalchemy as sa
import sqlalchemy.orm

from psycopg2_mq import MQSource, make_default_model

metadata = sa.MetaData()
model = make_default_model(metadata)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--url', required=True)
    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG)

    engine = sa.create_engine(args.url)
    metadata.create_all(engine)

    with sa.orm.Session(engine) as db:
        source = MQSource(dbsession=db, model=model)

        listener = source.add_listener(
            'mq_job_complete.dummy.echo',
            'dummy',
            'listener_echo',
            {'source': 'echo-listener'},
            cursor_key='echo-listener',
            collapse_on_cursor=True,
            when=timedelta(seconds=30),
        )
        print('listener', listener)

        schedule = source.add_schedule(
            'dummy', 'echo', {'source': 'minutely'}, rrule='FREQ=MINUTELY'
        )
        print('schedule', schedule)

        schedule = source.add_schedule(
            'dummy', 'echo', {'source': 'secondly'}, rrule='FREQ=SECONDLY;INTERVAL=15'
        )
        print('schedule', schedule)
        db.commit()


if __name__ == '__main__':
    raise SystemExit(main())
