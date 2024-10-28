import argparse
import logging
import sqlalchemy as sa
import sqlalchemy.orm
import time

from psycopg2_mq import MQSource, make_default_model

metadata = sa.MetaData()
model = make_default_model(metadata)


def wait_for_job(source, job_id, *, max_wait=60):
    start = time.time()
    while True:
        if time.time() - start > max_wait:  # pragma: nocover
            raise Exception('timeout while waiting for job to finish', job_id)
        with source.dbsession.begin():
            job = source.get_job(job_id)
            if job.state in {
                source.model.JobStates.COMPLETED,
                source.model.JobStates.FAILED,
                source.model.JobStates.LOST,
            }:
                break
            time.sleep(0.1)


parser = argparse.ArgumentParser()
parser.add_argument('--url', required=True)
args = parser.parse_args()

logging.basicConfig(level=logging.DEBUG)

engine = sa.create_engine(args.url)
metadata.create_all(engine)

db = sa.orm.Session(engine)
source = MQSource(dbsession=db, model=model)

# continue with python -i
