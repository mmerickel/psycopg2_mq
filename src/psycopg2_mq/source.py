from datetime import datetime
import json
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert
from zope.sqlalchemy import mark_changed

from .util import datetime_to_int


log = __import__('logging').getLogger(__name__)


class MQSource:
    def __init__(
        self,
        *,
        dbsession,
        model,
        transaction_manager=None,
    ):
        self.dbsession = dbsession
        self.model = model
        self.transaction_manager = transaction_manager

    def call(
        self,
        queue,
        method,
        args,
        *,
        when=None,
        now=None,
        cursor_key=None,
        job_kwargs=None,
    ):
        if now is None:
            now = datetime.utcnow()
        if when is None:
            when = now
        if args is None:
            args = {}
        if job_kwargs is None:
            job_kwargs = {}

        Job = self.model.Job
        JobStates = self.model.JobStates

        job_id = None
        job_is_new = False
        # there is a race condition here in READ COMMITTED mode where
        # a job could start between the insert and the query, so if we
        # do not get a locked job record here we will loop until we either
        # insert a new row or get a locked one
        while job_id is None:
            result = self.dbsession.execute(
                insert(Job.__table__)
                .values(
                    queue=queue,
                    method=method,
                    args=args,
                    created_time=now,
                    scheduled_time=when,
                    state=JobStates.PENDING,
                    cursor_key=cursor_key,
                    **job_kwargs,
                )
                .on_conflict_do_nothing(
                    index_elements=[Job.cursor_key],
                    index_where=(Job.state == JobStates.PENDING),
                )
                .returning(Job.id)
            )
            job_id, = next(result, (None,))

            # we just inserted a new job, notify workers
            if job_id is not None:
                job_is_new = True
                break

            # a job already exists, so let's find it and return the id
            # we lock the job in the pending state to ensure that it doesn't
            # start until our transaction completes with data that should be
            # used in the worker
            job_id = (
                self.dbsession.query(Job.id)
                .with_for_update()
                .filter(
                    Job.state == JobStates.PENDING,
                    Job.cursor_key == cursor_key,
                )
                .scalar()
            )

        if job_is_new:
            epoch_seconds = datetime_to_int(when)
            payload = json.dumps({'j': job_id, 't': epoch_seconds})
            self.dbsession.execute(sa.select([
                sa.func.pg_notify(queue, payload),
            ]))
            if self.transaction_manager:
                mark_changed(self.dbsession)

            log.info('enqueuing job=%s on queue=%s, method=%s',
                     job_id, queue, method)

        else:
            log.debug('joining existing job=%s', job_id)

        return job_id

    @property
    def query(self):
        return self.dbsession.query(self.model.Job)

    def find_job(self, job_id):
        return self.query.get(job_id)

    def retry(self, job_id):
        job = self.find_job(job_id)
        if job is None or job.state not in {
            self.model.JobStates.FAILED,
            self.model.JobStates.LOST,
        }:
            raise RuntimeError('job is not finished, cannot retry')
        return self.call(
            job.queue, job.method, job.args,
            when=job.scheduled_time,
            cursor_key=job.cursor_key,
        )
