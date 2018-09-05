from datetime import datetime
import json
import sqlalchemy as sa

from .util import datetime_to_int


log = __import__('logging').getLogger(__name__)


class MQSource:
    def __init__(
        self,
        *,
        dbsession,
        model,
    ):
        self.dbsession = dbsession
        self.model = model

    def call(self, queue, method, args, when=None, now=None):
        if now is None:
            now = datetime.utcnow()
        if when is None:
            when = now
        if args is None:
            args = {}
        job = self.model.Job(
            queue=queue,
            method=method,
            args=args,
            created_time=now,
            scheduled_time=when,
            state=self.model.JobStates.PENDING,
        )
        self.dbsession.add(job)
        self.dbsession.flush()

        epoch_seconds = datetime_to_int(job.scheduled_time)
        payload = json.dumps({'j': job.id, 't': epoch_seconds})
        self.dbsession.execute(
            sa.select([sa.func.pg_notify(job.queue, payload)]),
        )

        log.info('enqueuing job=%s on queue=%s, method=%s',
                 job.id, queue, method)
        return job

    @property
    def query(self):
        return self.dbsession.query(self.model.Job)

    def find_job(self, id):
        return self.query.get(id)

    def retry(self, job_id):
        job = self.find_job(job_id)
        if job is None or job.state not in {
            self.model.JobStates.RUNNING,
            self.model.JobStates.FAILED,
            self.model.JobStates.LOST,
        }:
            raise RuntimeError('job is not finished, cannot retry')
        return self.call(
            job.queue, job.method, job.args,
            when=job.scheduled_time,
        )
