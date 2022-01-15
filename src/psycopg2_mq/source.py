from datetime import datetime
import json
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert
from zope.sqlalchemy import mark_changed

from .util import datetime_to_int, get_next_rrule_time


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
        schedule_id=None,
        collapse_on_cursor=True,
    ):
        """
        Dispatch a new job.

        :param conflict_resolver:
            A callable that accepts the old job as its only argument. This will
            be invoked if ``collapse_on_cursor`` is ``True`` and a job already
            exists. The callable can modify the job object prior to returning
            to update any arguments prior to it running.

        :param collapse_on_cursor:
            If set to ``False``, then new jobs will always be created.
            When ``True`` and there is already a job on this cursor in the
            pending state with the same queue/method and marked collapsible then
            the ``conflict_resolver`` will be invoked with the existing job and
            the new request will be collapsed into the previous pending job.

        """
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

        collapsible = cursor_key is not None and collapse_on_cursor

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
                    Job.schedule_id: schedule_id,
                    Job.collapsible: collapsible,
                    **job_kwargs,
                })
                .on_conflict_do_nothing(
                    index_elements=[Job.cursor_key, Job.queue, Job.method],
                    index_where=sa.and_(
                        Job.state == JobStates.PENDING,
                        Job.collapsible == sa.true(),
                    ),
                )
                .returning(Job.id)
            ).scalar()
            if job_id is not None:
                if schedule_id is not None:
                    log.info(
                        'created new job=%s on queue=%s, method=%s from schedule=%s',
                        job_id, queue, method, schedule_id,
                    )
                else:
                    log.info(
                        'created new job=%s on queue=%s, method=%s',
                        job_id, queue, method,
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
                    Job.queue == queue,
                    Job.method == method,
                    Job.cursor_key == cursor_key,
                    Job.collapsible == sa.true(),
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
        """
        Retry a job.

        This dispatches a new job, copying the metadata from a job that was
        failed or lost.

        """
        job = self.find_job(job_id)
        if job is None or job.state not in {
            self.model.JobStates.COMPLETED,
            self.model.JobStates.FAILED,
            self.model.JobStates.LOST,
        }:
            raise RuntimeError('job is not in a retryable state')
        return self.call(
            job.queue, job.method, job.args,
            when=job.scheduled_time,
            cursor_key=job.cursor_key,
            schedule_id=job.schedule_id,
        )

    def reload_scheduler(self, queue, *, now=None):
        """
        Trigger the scheduler to reload information about modified schedules.

        For example, this should be used if ``reload=False`` is used with
        :ref:`.add_schedule`, :ref:`.disable_schedule`, or
        :ref:`.enable_schedule` in order to batch changes prior to reloading.

        """
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
        """
        Add a new schedule record to dispatch jobs at a specified frequency.

        """
        if now is None:
            now = datetime.utcnow()
        if args is None:
            args = {}
        if schedule_kwargs is None:
            schedule_kwargs = {}

        next_execution_time = get_next_rrule_time(rrule, now, now)

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
        """
        Disable a schedule preventing any further jobs from being automatically
        dispatched.

        """
        schedule = self.get_schedule(schedule_id, for_update=True)
        if not schedule.is_enabled:
            log.info('schedule=%s is already disabled', schedule_id)
            return
        schedule.is_enabled = False
        schedule.next_execution_time = None
        log.debug('disabled schedule=%s', schedule_id)

    def enable_schedule(self, schedule_id, *, now=None, reload=True):
        """ Enable a schedule for execution at its next scheduled time."""
        if now is None:
            now = datetime.utcnow()
        schedule = self.get_schedule(schedule_id, for_update=True)
        if schedule.is_enabled:
            log.info('schedule=%s is already enabled', schedule_id)
            return
        schedule.is_enabled = True
        schedule.next_execution_time = get_next_rrule_time(
            schedule.rrule, schedule.created_time, now)
        if reload and schedule.next_execution_time is not None:
            self.reload_scheduler(schedule.queue, now=schedule.next_execution_time)
        log.debug(
            'enabling schedule=%s, next execution time=%s',
            schedule_id, schedule.next_execution_time,
        )

    def call_schedule(self, schedule_id, *, now=None, reload=True, when=None):
        """
        Manually invoke a schedule, dispatching a job immediately.

        This is useful when a job may have been skipped because no schedulers
        were active at the time it was due to execute.

        """
        if now is None:
            now = datetime.utcnow()
        if when is None:
            when = now

        schedule = self.get_schedule(schedule_id, for_update=True)
        job_id = self.call(
            queue=schedule.queue,
            method=schedule.method,
            args=schedule.args,
            cursor_key=schedule.cursor_key,
            now=now,
            when=when,
            schedule_id=schedule.id,
        )

        if schedule.is_enabled:
            schedule.next_execution_time = get_next_rrule_time(
                schedule.rrule, schedule.created_time, now)

            if reload and schedule.next_execution_time is not None:
                self.reload_scheduler(schedule.queue, now=schedule.next_execution_time)

        return job_id
