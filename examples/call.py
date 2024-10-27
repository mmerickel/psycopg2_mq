import argparse
from contextlib import contextmanager
import sqlalchemy as sa
import sqlalchemy.orm
import time

from psycopg2_mq import MQSource, make_default_model

metadata = sa.MetaData()
model = make_default_model(metadata)


@contextmanager
def wait_for_job(source, job_id, *, max_wait=60):
    start = time.time()
    while True:
        if time.time() - start > max_wait:  # pragma: nocover
            raise Exception('timeout while waiting for job to finish', job_id)
        with source.dbsession.begin():
            job = source.find_job(job_id)
            if job.state in {
                source.model.JobStates.COMPLETED,
                source.model.JobStates.FAILED,
                source.model.JobStates.LOST,
            }:
                yield job
                break
            time.sleep(0.1)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--url', required=True)
    parser.add_argument('--no-wait', action='store_true')
    args = parser.parse_args()

    engine = sa.create_engine(args.url)
    metadata.create_all(engine)

    with sa.orm.Session(engine) as db:
        source = MQSource(dbsession=db, model=model)

        job_id = source.call('dummy', 'echo', {'time': time.time()})
        print('job', job_id)
        db.commit()

        if not args.no_wait:
            with wait_for_job(source, job_id) as job:
                print('state', job.state)
                print('result', job.result)


if __name__ == '__main__':
    raise SystemExit(main())
