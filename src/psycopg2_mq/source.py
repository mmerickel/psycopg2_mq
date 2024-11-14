from copy import deepcopy
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
        job_kwargs=None,
        cursor_key=None,
        collapse_on_cursor=False,
        conflict_resolver=None,
        schedule_ids=None,
        listener_ids=None,
        trace=None,
    ):
        """
        Dispatch a new job.

        :param collapse_on_cursor:
            If set to ``False``, then new jobs will always be created.
            When ``True`` and there is already a job on this cursor in the
            pending state with the same queue/method and marked collapsible then
            the ``conflict_resolver`` will be invoked with the existing job and
            the new request will be collapsed into the previous pending job.

            By default this is always ``False``.

        :param conflict_resolver:
            A callable that accepts the old job as its only argument. This will
            be invoked if ``collapse_on_cursor`` is ``True`` and a job already
            exists. The callable can modify the job object prior to returning
            to update any arguments prior to it running.

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
        JobListenerLink = self.model.JobListenerLink
        JobScheduleLink = self.model.JobScheduleLink
        JobStates = self.model.JobStates

        if collapse_on_cursor and not cursor_key:
            raise ValueError('cannot collapse a job that is not using a cursor')

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
                    {
                        Job.queue: queue,
                        Job.method: method,
                        Job.args: args,
                        Job.created_time: now,
                        Job.scheduled_time: when,
                        Job.state: JobStates.PENDING,
                        Job.cursor_key: cursor_key,
                        Job.collapsible: collapse_on_cursor,
                        Job.trace: trace,
                        **job_kwargs,
                    }
                )
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
                if schedule_ids:
                    log.info(
                        'created new job=%s on queue=%s, method=%s from schedule=%s',
                        job_id,
                        queue,
                        method,
                        schedule_ids,
                    )
                elif listener_ids:
                    log.info(
                        'created new job=%s on queue=%s, method=%s from listener=%s',
                        job_id,
                        queue,
                        method,
                        listener_ids,
                    )
                else:
                    log.info(
                        'created new job=%s on queue=%s, method=%s',
                        job_id,
                        queue,
                        method,
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

        if listener_ids and JobListenerLink is not None:
            self.dbsession.execute(
                insert(JobListenerLink.__table__)
                .values(
                    [
                        {
                            JobListenerLink.job_id: job_id,
                            JobListenerLink.listener_id: listener_id,
                        }
                        for listener_id in listener_ids
                    ]
                )
                .on_conflict_do_nothing(
                    index_elements=[
                        JobListenerLink.job_id,
                        JobListenerLink.listener_id,
                    ],
                )
            )

        if schedule_ids and JobScheduleLink is not None:
            self.dbsession.execute(
                insert(JobScheduleLink.__table__)
                .values(
                    [
                        {
                            JobScheduleLink.job_id: job_id,
                            JobScheduleLink.schedule_id: schedule_id,
                        }
                        for schedule_id in schedule_ids
                    ]
                )
                .on_conflict_do_nothing(
                    index_elements=[
                        JobScheduleLink.job_id,
                        JobScheduleLink.schedule_id,
                    ],
                )
            )

        if notify:
            epoch_seconds = datetime_to_int(when)
            payload = json.dumps({'j': job_id, 't': epoch_seconds})
            self.dbsession.execute(
                sa.select(
                    sa.func.pg_notify(f'{self.model.channel_prefix}{queue}', payload),
                )
            )
            # XXX notify is always true when the raw insert works above so we
            # handle the two scenarios (notify and raw insert) in which
            # mark_changed needs to be called with only a single call
            if self.transaction_manager:
                mark_changed(self.dbsession)

        return job_id

    @property
    def query_jobs(self):
        return self.dbsession.query(self.model.Job)

    def get_job(self, job_id, *, for_update=False, raises=True):
        if not for_update:
            job = self.dbsession.get(self.model.Job, job_id)
        else:
            job = self.query_jobs.filter_by(id=job_id).with_for_update().one_or_none()
        if not job and raises:
            raise LookupError('cannot find job', job_id)
        return job

    def retry_job(self, job_id, *, now=None):
        """
        Retry a job.

        This dispatches a new job by copying the metadata from a job that was finished.

        It is possible to retry a job regardless of the current job's state.

        The new job is never collapsible with other jobs. If you wish to retry it as
        collapsible then you should invoke :meth:`.call` directly yourself.

        """
        if now is None:
            now = datetime.utcnow()

        job = self.get_job(job_id)
        kw = {}
        if self.model.JobListenerLink is not None:
            kw['listener_ids'] = [link.listener_id for link in job.listener_links]
        if self.model.JobScheduleLink is not None:
            kw['schedule_ids'] = [link.schedule_id for link in job.schedule_links]

        trace = deepcopy(job.trace or {})
        trace['mq_source_job_id'] = job_id

        return self.call(
            job.queue,
            job.method,
            job.args,
            now=now,
            when=job.scheduled_time,
            cursor_key=job.cursor_key,
            collapse_on_cursor=False,
            trace=trace,
            **kw,
        )

    def fail_lost_job(self, job_id, *, result=None):
        """
        Mark a lost job as failed.

        """
        job = self.get_job(job_id, for_update=True)
        if job.state != self.model.JobStates.LOST:
            raise RuntimeError('job is not in a lost state')

        job.state = self.model.JobStates.FAILED
        if result is not None:
            job.result = result

        self.emit_event(
            f'mq.job_finished.failed.{job.queue}.{job.method}',
            {
                'id': job_id,
                'queue': job.queue,
                'method': job.method,
                'start_time': job.start_time.isoformat(),
                'end_time': job.end_time.isoformat(),
                'result': result,
            },
        )

    def cancel_job(self, job_id, *, now=None):
        """
        Cancel a job.

        Only pending and failed jobs can be marked as canceled. It is not possible to
        cancel a job that is currently running.

        """
        if now is None:
            now = datetime.utcnow()

        job = self.get_job(job_id, for_update=True)
        if job.state not in {
            self.model.JobStates.PENDING,
            self.model.JobStates.FAILED,
        }:
            raise RuntimeError('job is not in a valid state')

        job.state = self.model.JobStates.CANCELED
        if job.end_time is None:
            job.end_time = now
        log.info('marked job=%s canceled', job_id)

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
        self.dbsession.execute(
            sa.select(
                sa.func.pg_notify(f'{self.model.channel_prefix}{queue}', payload),
            )
        )

    def add_schedule(
        self,
        queue,
        method,
        args,
        *,
        rrule,
        is_enabled=True,
        cursor_key=None,
        collapse_on_cursor=False,
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

        if collapse_on_cursor and not cursor_key:
            raise ValueError(
                'cannot collapse jobs from a schedule that is not using a cursor'
            )

        next_execution_time = get_next_rrule_time(rrule, now, now)

        schedule = self.model.JobSchedule(
            queue=queue,
            method=method,
            args=args,
            rrule=rrule,
            is_enabled=is_enabled,
            cursor_key=cursor_key,
            collapse_on_cursor=collapse_on_cursor,
            created_time=now,
            next_execution_time=next_execution_time,
            **schedule_kwargs,
        )
        self.dbsession.add(schedule)
        self.dbsession.flush()

        if reload:
            self.reload_scheduler(queue, now=schedule.next_execution_time)

        return schedule

    @property
    def query_schedules(self):
        return self.dbsession.query(self.model.JobSchedule)

    def get_schedule(self, schedule_id, *, for_update=False):
        q = self.query_schedules.filter_by(id=schedule_id)
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
        """Enable a schedule for execution at its next scheduled time."""
        if now is None:
            now = datetime.utcnow()
        schedule = self.get_schedule(schedule_id, for_update=True)
        if schedule.is_enabled:
            log.info('schedule=%s is already enabled', schedule_id)
            return
        schedule.is_enabled = True
        schedule.next_execution_time = get_next_rrule_time(
            schedule.rrule, schedule.created_time, now
        )
        if reload and schedule.next_execution_time is not None:
            self.reload_scheduler(schedule.queue, now=schedule.next_execution_time)
        log.debug(
            'enabling schedule=%s, next execution time=%s',
            schedule_id,
            schedule.next_execution_time,
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
            collapse_on_cursor=schedule.collapse_on_cursor,
            now=now,
            when=when,
            schedule_ids=[schedule.id],
        )

        if schedule.is_enabled:
            schedule.next_execution_time = get_next_rrule_time(
                schedule.rrule, schedule.created_time, now
            )

            if reload and schedule.next_execution_time is not None:
                self.reload_scheduler(schedule.queue, now=schedule.next_execution_time)

        return job_id

    def add_listener(
        self,
        event,
        queue,
        method,
        args,
        *,
        when=None,
        is_enabled=True,
        context_arg_key=None,
        cursor_key=None,
        collapse_on_cursor=False,
        listener_kwargs=None,
        now=None,
    ):
        """
        Add a new listener that can dispatch jobs when an event is emitted.

        """
        if now is None:
            now = datetime.utcnow()
        if args is None:
            args = {}
        if listener_kwargs is None:
            listener_kwargs = {}

        if collapse_on_cursor and not cursor_key:
            raise ValueError(
                'cannot collapse jobs from a schedule that is not using a cursor'
            )

        if not context_arg_key:
            context_arg_key = 'events' if collapse_on_cursor else 'event'

        listener = self.model.JobListener(
            event=event,
            queue=queue,
            method=method,
            args=args,
            context_arg_key=context_arg_key,
            when=when,
            is_enabled=is_enabled,
            cursor_key=cursor_key,
            collapse_on_cursor=collapse_on_cursor,
            created_time=now,
            **listener_kwargs,
        )
        self.dbsession.add(listener)
        self.dbsession.flush()

        return listener

    @property
    def query_listeners(self):
        return self.dbsession.query(self.model.JobListener)

    def get_listener(self, listener_id, *, for_update=False):
        q = self.query_listeners.filter_by(id=listener_id)
        if for_update:
            q = q.with_for_update()
        return q.one()

    def disable_listener(self, listener_id):
        """
        Disable a listener preventing any further jobs from being automatically
        dispatched.

        """
        listener = self.get_listener(listener_id, for_update=True)
        if not listener.is_enabled:
            log.info('listener=%s is already disabled', listener_id)
            return
        listener.is_enabled = False
        log.debug('disabled listener=%s', listener_id)

    def enable_listener(self, listener_id, *, now=None):
        """Enable a listener for execution when the next event is dispatched."""
        if now is None:
            now = datetime.utcnow()
        listener = self.get_listener(listener_id, for_update=True)
        if listener.is_enabled:
            log.info('listener=%s is already enabled', listener_id)
            return
        listener.is_enabled = True
        log.debug('enabling listener=%s', listener_id)

    def emit_event(self, name, data, *, now=None, trace=None):
        if now is None:
            now = datetime.utcnow()

        job_ids = []
        for listener in self.query_listeners.filter(
            self.model.JobListener.event == name,
            self.model.JobListener.is_enabled,
        ):
            when = now
            if listener.when is not None:
                when += listener.when

            arg_key = listener.context_arg_key

            event = {
                'name': name,
                'listener_id': listener.id,
                'data': data,
            }

            if listener.collapse_on_cursor:
                job_args = {
                    arg_key: [event],
                    **listener.args,
                }

                def conflict_resolver(job, arg_key=arg_key, event=event):
                    job.args = deepcopy(job.args)
                    job.args.setdefault(arg_key, []).append(event)

            else:
                conflict_resolver = None
                job_args = {
                    arg_key: event,
                    **listener.args,
                }

            job_id = self.call(
                queue=listener.queue,
                method=listener.method,
                args=job_args,
                cursor_key=listener.cursor_key,
                collapse_on_cursor=listener.collapse_on_cursor,
                conflict_resolver=conflict_resolver,
                now=now,
                when=when,
                listener_ids=[listener.id],
                trace=trace,
            )
            job_ids.append(job_id)
        return job_ids
