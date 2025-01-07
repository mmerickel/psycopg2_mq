import argparse
import logging
from pprint import pprint
import sqlalchemy as sa
import sqlalchemy.orm
import time

from psycopg2_mq import MQWorker, make_default_model

metadata = sa.MetaData()
model = make_default_model(metadata)


class DummyQueue:
    def __init__(self, engine):
        self.dbmaker = sa.orm.sessionmaker(bind=engine)

    def execute_job(self, job):
        db = self.dbmaker()
        with db:
            db.execute(sa.text('select 1')).all()
            if job.method == 'sleep':
                duration = job.args['duration']
                print(f'sleeping {duration}')
                time.sleep(duration)
            result = {
                'id': job.id,
                'queue': job.queue,
                'method': job.method,
                'args': job.args,
                'listener_ids': job.listener_ids,
                'schedule_ids': job.schedule_ids,
                'trace': job.trace,
            }
            pprint(result)
            return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--url', required=True)
    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG)

    engine = sa.create_engine(args.url)
    metadata.create_all(engine)

    queues = {
        'dummy': DummyQueue(engine),
    }

    worker = MQWorker(
        threads=2,
        engine=engine,
        queues=queues,
        model=model,
    )
    worker.run()


if __name__ == '__main__':
    raise SystemExit(main())
