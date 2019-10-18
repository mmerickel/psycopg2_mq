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
        conflict_resolver=None,
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
        notify = False
        # there is a race condition here in READ COMMITTED mode where
        # a job could start between the insert and the query, so if we
        # do not get a locked job record here we will loop until we either
        # insert a new row or get a locked one
        while True:
            job_id = self.dbsession.execute(
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
            ).scalar()
            if job_id is not None:
                log.info('scheduled new job=%s on queue=%s, method=%s',
                         job_id, queue, method)
                notify = True
                break

            # a job already exists, load it and resolve conflicts
            # XXX this will only occur on jobs with a cursor
            job = (
                self.dbsession.query(Job)
                .with_for_update()
                .filter(
                    Job.state == JobStates.PENDING,
                    Job.cursor_key == cursor_key,
                )
                .first()
            )
            if job is not None:
                job_id = job.id
                log.info('joining existing job=%s', job_id)

                # the earlier scheduled_time should always win
                if when < job.scheduled_time:
                    job.scheduled_time = when
                    notify = True

                # invoke a callable to update properties on the job
                if conflict_resolver is not None:
                    conflict_resolver(job)

                break

        if notify:
            epoch_seconds = datetime_to_int(when)
            payload = json.dumps({'j': job_id, 't': epoch_seconds})
            self.dbsession.execute(sa.select([
                sa.func.pg_notify(queue, payload),
            ]))
            # XXX notify is always true when the raw insert works above so we
            # handle the two scenarios (notify and raw insert) in which
            # mark_changed needs to be called with only a single call
            if self.transaction_manager:
                mark_changed(self.dbsession)

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
