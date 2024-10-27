import argparse
import logging
import sqlalchemy as sa

from psycopg2_mq import MQWorker, make_default_model

metadata = sa.MetaData()
model = make_default_model(metadata)


class DummyQueue:
    def execute_job(self, job):
        print(job.id, job.queue, job.method, job.args)
        return {
            'queue': job.queue,
            'method': job.method,
            'args': job.args,
        }


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
