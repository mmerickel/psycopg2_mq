import argparse
import logging
from pprint import pprint
import sqlalchemy as sa

from psycopg2_mq import MQWorker, make_default_model

metadata = sa.MetaData()
model = make_default_model(metadata)


class DummyQueue:
    def execute_job(self, job):
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
        'dummy': DummyQueue(),
    }

    worker = MQWorker(
        engine=engine,
        queues=queues,
        model=model,
    )
    worker.run()


if __name__ == '__main__':
    raise SystemExit(main())
