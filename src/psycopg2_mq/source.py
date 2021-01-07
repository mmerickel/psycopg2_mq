from datetime import datetime
from dateutil.rrule import rrulestr
from dateutil.tz import UTC
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

        schedule_id = job_kwargs.get('schedule_id')

        job_id = None
        notify = False
        # there is a race condition here in READ COMMITTED mode where
        # a job could start between the insert and the query, so if we
        # do not get a locked job record here we will loop until we either
        # insert a new row or get a locked one
        while True:
            job_id = self.dbsession.execute(
                insert(Job.__table__)
                .values({
                    Job.queue: queue,
                    Job.method: method,
                    Job.args: args,
                    Job.created_time: now,
                    Job.scheduled_time: when,
                    Job.state: JobStates.PENDING,
                    Job.cursor_key: cursor_key,
                    **job_kwargs,
                })
                .on_conflict_do_nothing(
                    index_elements=[Job.cursor_key],
                    index_where=(Job.state == JobStates.PENDING),
                )
                .returning(Job.id)
            ).scalar()
            if job_id is not None:
                if schedule_id is not None:
                    log.info(
                        f'created new job={job_id} on queue={queue}, '
                        f'method={method} from schedule={schedule_id}'
                    )
                else:
                    log.info(
                        f'created new job={job_id} on queue={queue}, '
                        f'method={method}'
                    )
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

                    if self.transaction_manager:
                        mark_changed(self.dbsession)

                break

        if notify:
            epoch_seconds = datetime_to_int(when)
            payload = json.dumps({'j': job_id, 't': epoch_seconds})
            self.dbsession.execute(sa.select([
                sa.func.pg_notify(f'{self.model.channel_prefix}{queue}', payload),
            ]))
            # XXX notify is always true when the raw insert works above so we
            # handle the two scenarios (notify and raw insert) in which
            # mark_changed needs to be called with only a single call
            if self.transaction_manager:
                mark_changed(self.dbsession)

        return job_id

    @property
    def query_job(self):
        return self.dbsession.query(self.model.Job)

    def find_job(self, job_id):
        return self.query_job.get(job_id)

    def retry_job(self, job_id):
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

    def reload_scheduler(self, queue, *, now=None):
        if now is None:
            now = datetime.utcnow()
        epoch_seconds = datetime_to_int(now)
        payload = json.dumps({'s': None, 't': epoch_seconds})
        self.dbsession.execute(sa.select([
            sa.func.pg_notify(f'{self.model.channel_prefix}{queue}', payload),
        ]))

    def add_schedule(
        self,
        queue,
        method,
        args,
        *,
        rrule,
        is_enabled=True,
        cursor_key=None,
        schedule_kwargs=None,
        now=None,
        reload=True,
    ):
        if now is None:
            now = datetime.utcnow()
        if args is None:
            args = {}
        if schedule_kwargs is None:
            schedule_kwargs = {}

        next_execution_time = get_next_schedule_execution_time(rrule, now, now)

        schedule = self.model.JobSchedule(
            queue=queue,
            method=method,
            args=args,
            rrule=rrule,
            is_enabled=is_enabled,
            cursor_key=cursor_key,
            created_time=now,
            next_execution_time=next_execution_time,
            **schedule_kwargs,
        )
        self.dbsession.add(schedule)

        if reload:
            self.reload_scheduler(queue, now=schedule.next_execution_time)
        return schedule

    @property
    def query_schedule(self):
        return self.dbsession.query(self.model.JobSchedule)

    def get_schedule(self, schedule_id, *, for_update=False):
        q = self.query_schedule.filter_by(id=schedule_id)
        if for_update:
            q = q.with_for_update()
        return q.one()

    def disable_schedule(self, schedule_id):
        schedule = self.get_schedule(schedule_id, for_update=True)
        schedule.is_enabled = False
        log.debug(f'disabled schedule={schedule_id}')

    def enable_schedule(self, schedule_id, *, now=None, reload=True):
        if now is None:
            now = datetime.utcnow()
        schedule = self.get_schedule(schedule_id, for_update=True)
        schedule.is_enabled = True
        schedule.next_execution_time = get_next_schedule_execution_time(
            schedule.rrule, schedule.created_time, now)
        if reload and schedule.next_execution_time is not None:
            self.reload_schedule_queue(schedule.queue, now=schedule.next_execution_time)
        log.debug(
            f'enabling schedule={schedule_id}, '
            f'next execution time={schedule.next_execution_time}'
        )

    def _apply_schedule(self, schedule, *, now=None):
        job_id = self.call(
            queue=schedule.queue,
            method=schedule.method,
            args=schedule.args,
            cursor_key=schedule.cursor_key,
            now=now,
            when=schedule.next_execution_time,
            job_kwargs=dict(schedule_id=schedule.id),
        )

        schedule.next_execution_time = get_next_schedule_execution_time(
            schedule.rrule, schedule.created_time, now)
        return job_id


def get_next_schedule_execution_time(rrule, dtstart, after):
    rrule = rrulestr(rrule, dtstart=dtstart)
    try:
        ts = rrule.after(after)
    except Exception:
        # we do not know if the rrule's dtstart is timezone-aware or not
        # and dateutil doesn't allow us to provide a default of UTC and
        # so what we do is try again with a tz-aware object
        after = after.replace(tzinfo=UTC)
        ts = rrule.after(after)

    if ts is not None and ts.tzinfo is not None:
        ts = ts.astimezone(UTC).replace(tzinfo=None)
    return ts
