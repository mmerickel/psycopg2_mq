from concurrent import futures
from contextlib import contextmanager
from datetime import datetime, timedelta
import functools
import json
import os
import platform
import psycopg2
import random
import select
import signal
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session
from sqlalchemy.sql import table, column
import sys
import threading
import traceback

from .source import MQSource
from .trigger import Trigger
from .util import (
    class_name,
    get_next_rrule_time,
    int_to_datetime,
    safe_object,
)


DEFAULT_TIMEOUT = 60
DEFAULT_JITTER = 1
DEFAULT_LOCK_KEY = 1250360252  # selected via random.randint(0, 2**31-1)


log = __import__('logging').getLogger(__name__)


pg_locks = table(
    'pg_locks',
    column('locktype', sa.Text()),
    column('classid', sa.Integer()),
    column('objid', sa.Integer()),
)


class JobContext:
    def __init__(
        self,
        *,
        id,
        queue,
        method,
        args,
        cursor_key=None,
        cursor=None,
        schedule_id=None,
    ):
        self.id = id
        self.queue = queue
        self.method = method
        self.args = args
        self.cursor_key = cursor_key
        self.cursor = cursor
        self.schedule_id = schedule_id

    def extend(self, **kw):
        for k, v in kw.items():
            setattr(self, k, v)


class MQWorker:
    _now = datetime.utcnow  # for testing

    def __init__(
        self,
        *,
        engine,
        queues,
        model,

        timeout=DEFAULT_TIMEOUT,
        jitter=DEFAULT_JITTER,
        lock_key=DEFAULT_LOCK_KEY,
        threads=1,
        capture_signals=True,
        name=None,
    ):
        self._engine = engine
        self._queues = queues
        self._model = model

        if not isinstance(timeout, timedelta):
            timeout = timedelta(seconds=timeout)
        self._timeout = timeout
        self._jitter = jitter
        self._lock_key = lock_key
        self._threads = threads
        self._capture_signals = capture_signals

        if name is None:
            name = guess_worker_name()
        self._name = name

        self._running = False

        self._connect_time = None
        self._dbconn = None
        self._lock_id = None
        self._shutdown_trigger = None
        self._job_trigger = None
        self._next_job_time = datetime.max
        self._next_schedule_time = datetime.max
        self._next_maintenance_time = datetime.min
        self._pool = None
        self._active_jobs = {}
        self._scheduler_queues = set()

    def shutdown_gracefully(self):
        self._running = False
        if self._shutdown_trigger:
            self._shutdown_trigger.notify()

    def run(self):
        self._running = True
        try:
            log.info('starting mq worker=%s', self._name)
            with maybe_capture_signals(self):
                with connect_pool(self):
                    with connect_job_trigger(self):
                        with connect_shutdown_trigger(self):
                            with connect_db(self):
                                eventloop(self)

        finally:
            self._running = False

    def result_from_error(self, ex):
        return {
            'exc': class_name(ex.__class__),
            'args': safe_object(ex.args),
            'tb': traceback.format_tb(ex.__traceback__),
        }

    def get_status(self):
        # shallow copy to avoid iterating a shared reference
        active_jobs = self._active_jobs.copy()
        return {
            'name': self._name,
            'running': self._running,
            'total_threads': self._threads,
            'idle_threads': self._threads - len(active_jobs),
            'active_jobs': [
                {
                    'id': j.id,
                    'queue': j.queue,
                    'method': j.method,
                    'args': j.args,
                    'cursor_key': j.cursor_key,
                }
                for j in active_jobs.values()
            ],
        }


def engine_from_sessionmaker(maker):
    return maker.kw['bind']


def guess_worker_name():
    hostname = platform.node()
    pid = os.getpid()
    return f'{hostname}.{pid}'


@contextmanager
def maybe_capture_signals(ctx):
    if not ctx._capture_signals:
        yield

    def onsigterm(*args):
        # heroku may send multiple sigterm and we want to only respond
        # to the first one so we'll ignore all the others here
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        log.info('received SIGTERM, triggering workers to shutdown')
        ctx.shutdown_gracefully()

    def onsigint(*args):
        # allow another sigint to trigger a KeyboardInterrupt
        signal.signal(signal.SIGINT, signal.SIG_DFL)
        log.info('received Ctrl-C, waiting for workers to complete')
        ctx.shutdown_gracefully()

    def onsigusr1(*args):
        status = ctx.get_status()
        print(json.dumps(status, sort_keys=True, indent=2), file=sys.stderr)

    signal.signal(signal.SIGTERM, onsigterm)
    signal.signal(signal.SIGINT, onsigint)
    signal.signal(signal.SIGUSR1, onsigusr1)
    try:
        yield

    finally:
        signal.signal(signal.SIGUSR1, signal.SIG_DFL)
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        signal.signal(signal.SIGINT, signal.SIG_DFL)


@contextmanager
def connect_db(ctx):
    rotate_dbconn(ctx)
    try:

        yield

    finally:
        if ctx._dbconn is not None:
            try:
                release_worker_locks(ctx, retry=False)
            except Exception:
                log.warning(
                    'failed to release locks, they will be cleaned up by '
                    'another worker'
                )
            ctx._dbconn.close()
            ctx._dbconn = None


@contextmanager
def connect_shutdown_trigger(ctx):
    ctx._shutdown_trigger = Trigger()
    try:
        yield

    finally:
        ctx._shutdown_trigger.close()
        ctx._shutdown_trigger = None


@contextmanager
def connect_job_trigger(ctx):
    ctx._job_trigger = Trigger()
    try:
        yield

    finally:
        ctx._job_trigger.close()
        ctx._job_trigger = None


@contextmanager
def connect_pool(ctx):
    pool = futures.ThreadPoolExecutor(ctx._threads)
    ctx._pool = pool
    try:
        with pool:
            yield
    finally:
        ctx._pool = None


def rotate_dbconn(ctx):
    if ctx._dbconn is not None:
        ctx._dbconn.close()

    conn = ctx._dbconn = ctx._engine.connect()
    conn.detach()
    conn.execution_options(autocommit=True)
    assert conn.dialect.driver == 'psycopg2'
    curs = conn.connection.cursor()
    for queue in ctx._queues.keys():
        curs.execute(f'LISTEN {ctx._model.channel_prefix}{queue};')
    ctx._connect_time = ctx._now()
    log.info('connected')

    # clean up any locks acquired with the previous connection object
    release_stale_locks(ctx, retry=False)

    # grab an advisory lock which can track our later activity
    acquire_worker_lock(ctx, retry=False)

    # if we are executing some jobs then re-lock them so they aren't lost
    recover_active_jobs(ctx, retry=False)

    # we lost our locks on the scheduling queues so reinitilize
    recover_scheduler_queues(ctx, retry=False)

    # it's possible we missed some jobs while reconnecting so reinitialize
    set_next_job_time(ctx, retry=False)


def retry_dbconn(fn):
    @functools.wraps(fn)
    def wrapper(ctx, *args, **kwargs):
        retry = 'db' not in kwargs
        retry = kwargs.pop('retry', retry)
        # protect against misuse where db and retry=True are both supplied
        if 'db' in kwargs and retry:
            raise ValueError('cannot set retry when active db is manually supplied')
        try:
            return fn(ctx, *args, **kwargs)
        except sa.exc.DBAPIError as ex:
            if ex.connection_invalidated and retry:
                log.warning(
                    'connection closed unexpectedly, error="%s"',
                    str(ex).strip(),
                )
                rotate_dbconn(ctx)
                return fn(ctx, *args, **kwargs)
            raise
    return wrapper


def dbsession(fn):
    @retry_dbconn
    @functools.wraps(fn)
    def wrapper(ctx, *args, **kwargs):
        # support external db and acting as a passthrough
        if 'db' in kwargs:
            kwargs.setdefault('model', ctx._model)
            return fn(ctx, *args, **kwargs)
        # open a new session and commit/rollback when complete
        with ctx._dbconn.begin():
            db = Session(bind=ctx._dbconn)
            try:
                kwargs['db'] = db
                kwargs['model'] = ctx._model
                result = fn(ctx, *args, **kwargs)
                db.commit()
                return result
            finally:
                db.close()
    return wrapper


@dbsession
def run_maintenance(ctx, *, db, model):
    release_stale_locks(ctx, db=db, model=model)
    mark_lost_jobs(ctx, db=db, model=model)
    lock_scheduler_queues(ctx, db=db, model=model)
    set_next_job_time(ctx, db=db, model=model)

    ctx._next_maintenance_time = ctx._now() + ctx._timeout


@dbsession
def release_stale_locks(ctx, *, db, model):
    Lock = model.Lock
    lock_sq = (
        db.query(Lock.queue, Lock.key)
        # do not block rows that are being cleaned by another transaction
        .with_for_update(of=Lock, skip_locked=True)
        .outerjoin(pg_locks, sa.and_(
            pg_locks.c.locktype == 'advisory',
            pg_locks.c.classid == ctx._lock_key,
            pg_locks.c.objid == Lock.lock_id,
        ))
        .filter(
            Lock.queue.in_(ctx._queues.keys()),
            Lock.lock_id != sa.null(),
            pg_locks.c.objid == sa.null(),
        )
        .subquery()
    )
    count = (
        db.query(Lock)
        .filter(
            Lock.queue == lock_sq.c.queue,
            Lock.key == lock_sq.c.key,
        )
        .delete(synchronize_session=False)
    )
    if count > 0:
        log.warning('released %d stale locks', count)


@dbsession
def release_worker_locks(ctx, *, db, model):
    if ctx._lock_id is None:
        return

    Lock = model.Lock
    count = (
        db.query(Lock)
        .filter(
            Lock.lock_id == ctx._lock_id,
            Lock.worker == ctx._name,
        )
        .delete(synchronize_session=False)
    )
    if count > 0:
        log.debug('released %d locks', count)


@dbsession
def acquire_worker_lock(ctx, *, db, model, attempts=3):
    while attempts > 0:
        lock_id = random.randint(1, 2**31 - 1)
        is_locked = db.execute(
            'select pg_try_advisory_lock(:key, :id)',
            {'key': ctx._lock_key, 'id': lock_id},
        ).scalar()
        if is_locked:
            ctx._lock_id = lock_id
            log.debug('acquired worker lock=%s', ctx._lock_id)
            return

        attempts -= 1
    raise RuntimeError('failed to acquire unique worker advisory lock')


@dbsession
def claim_pending_job(ctx, *, now=None, db, model):
    if now is None:
        now = ctx._now()

    running_cursor_sq = (
        db.query(model.Job.cursor_key)
        .filter(
            model.Job.state == model.JobStates.RUNNING,
            model.Job.cursor_key.isnot(None),
        )
        .subquery()
    )
    job = (
        db.query(model.Job)
        .outerjoin(
            running_cursor_sq,
            running_cursor_sq.c.cursor_key == model.Job.cursor_key,
        )
        .with_for_update(of=model.Job, skip_locked=True)
        .filter(
            model.Job.state == model.JobStates.PENDING,
            model.Job.queue.in_(ctx._queues.keys()),
            model.Job.scheduled_time <= now,
            running_cursor_sq.c.cursor_key.is_(None),
        )
        .order_by(
            model.Job.scheduled_time.asc(),
            model.Job.created_time.asc(),
        )
        .first()
    )

    if job is not None:
        cursor = None
        if job.cursor_key is not None:
            cursor = (
                db.query(model.JobCursor)
                .filter(model.JobCursor.key == job.cursor_key)
                .first()
            )
            if cursor is not None:
                cursor = cursor.properties
            else:
                cursor = {}

            # since we commit before returning the context, it's safe to
            # not make a deep copy here because we know the job won't run
            # and mutate the content until after the snapshot is committed
            job.cursor_snapshot = cursor

        job.lock_id = ctx._lock_id
        job.state = model.JobStates.RUNNING
        job.start_time = now
        job.worker = ctx._name

        if job.schedule_id is not None:
            log.info(
                'beginning job=%s from schedule=%s %.3fs after scheduled start',
                job.id,
                job.schedule_id,
                (job.start_time - job.scheduled_time).total_seconds(),
            )
        else:
            log.info(
                'beginning job=%s %.3fs after scheduled start',
                job.id,
                (job.start_time - job.scheduled_time).total_seconds(),
            )
        return JobContext(
            id=job.id,
            queue=job.queue,
            method=job.method,
            args=job.args,
            cursor_key=job.cursor_key,
            cursor=cursor,
            schedule_id=job.schedule_id,
        )


def execute_job(queue, job):
    current_thread = threading.current_thread()
    old_thread_name = current_thread.name
    try:
        current_thread.name = f'{old_thread_name},job={job.id}'
        return queue.execute_job(job)

    except BaseException:
        log.exception('error while handling job=%s', job.id)
        raise

    finally:
        current_thread.name = old_thread_name


@dbsession
def finish_job(ctx, job_id, success, result, cursor=None, *, db, model):
    job = (
        db.query(model.Job)
        .with_for_update()
        .filter_by(id=job_id)
        .one()
    )

    if success:
        if job.cursor_key is not None:
            save_cursor(db, model, job.cursor_key, cursor)

        elif cursor is not None:
            log.warning('ignoring cursor for job=%s without a cursor_key', job_id)

    job.state = (
        model.JobStates.COMPLETED
        if success
        else model.JobStates.FAILED
    )
    job.result = result
    job.end_time = ctx._now()
    job.lock_id = None
    log.info('finished processing job=%s, state="%s"', job_id, job.state)


def save_cursor(db, model, key, cursor):
    if cursor is None:
        cursor = {}
    cursor_obj = (
        db.query(model.JobCursor)
        .filter(model.JobCursor.key == key)
        .with_for_update()
        .first()
    )
    if cursor_obj is None:
        cursor_obj = model.JobCursor(
            key=key,
            properties=cursor,
        )
        db.add(cursor_obj)

    elif cursor_obj.properties != cursor:
        cursor_obj.properties = cursor


@dbsession
def mark_lost_jobs(ctx, *, db, model):
    job_q = (
        db.query(model.Job)
        .with_for_update(of=model.Job)
        .outerjoin(pg_locks, sa.and_(
            pg_locks.c.locktype == 'advisory',
            pg_locks.c.classid == ctx._lock_key,
            pg_locks.c.objid == model.Job.lock_id,
        ))
        .filter(
            model.Job.state == model.JobStates.RUNNING,
            model.Job.queue.in_(ctx._queues.keys()),
            model.Job.lock_id != sa.null(),
            pg_locks.c.objid == sa.null(),
        )
        .order_by(model.Job.start_time.asc())
    )
    for job in job_q:
        job.state = model.JobStates.LOST
        job.lock_id = None
        job.end_time = ctx._now()
        log.error('marking job=%s as lost', job.id)


@dbsession
def recover_active_jobs(ctx, *, db, model):
    if not ctx._active_jobs:
        return

    job_q = (
        db.query(model.Job)
        .with_for_update(of=model.Job)
        .filter(model.Job.id.in_(j.id for j in ctx._active_jobs.values()))
        .order_by(model.Job.start_time.asc())
    )
    for job in job_q:
        # since the old connection is dead - the old lock is also gone,
        # we'll re-acquire a new one
        job.lock_id = ctx._lock_id
        job.state = model.JobStates.RUNNING
        log.info('recovering active job=%s', job.id)


@dbsession
def set_next_job_time(ctx, *, db, model):
    ctx._next_job_time = (
        db.query(sa.func.min(model.Job.scheduled_time))
        .filter(
            model.Job.state == model.JobStates.PENDING,
            model.Job.queue.in_(ctx._queues.keys()),
        )
        .scalar()
    )

    if ctx._next_job_time is None:
        log.debug('no pending jobs')
        ctx._next_job_time = datetime.max

    else:
        when = (ctx._next_job_time - ctx._now()).total_seconds()
        if when > 0:
            log.debug('tracking next job in %.3f seconds', when)


def enqueue_jobs(ctx):
    while ctx._running and len(ctx._active_jobs) < ctx._threads:
        job = claim_pending_job(ctx)
        if job is None:
            break
        queue = ctx._queues[job.queue]
        future = ctx._pool.submit(execute_job, queue, job)
        ctx._active_jobs[future] = job
        future.add_done_callback(lambda f: ctx._job_trigger.notify())

    if ctx._running:
        set_next_job_time(ctx)


def finish_jobs(ctx):
    done, _ = futures.wait(list(ctx._active_jobs.keys()), 0)
    for future in done:
        job = ctx._active_jobs.pop(future)
        error = future.exception()
        if error is not None:
            finish_job(ctx, job.id, False, ctx.result_from_error(error))

        else:
            result = future.result()
            finish_job(ctx, job.id, True, result, job.cursor)


@dbsession
def recover_scheduler_queues(ctx, *, db, model):
    ctx._scheduler_queues = set()
    lock_scheduler_queues(ctx, db=db, model=model)


@dbsession
def lock_scheduler_queues(ctx, *, db, model, now=None):
    if now is None:
        now = ctx._now()

    Lock = model.Lock
    new_queues = set()
    for queue in (q for q in ctx._queues if q not in ctx._scheduler_queues):
        result = db.execute(
            insert(Lock.__table__)
            .values({
                Lock.queue: queue,
                Lock.key: 'scheduler',
                Lock.lock_id: ctx._lock_id,
                Lock.worker: ctx._name,
            })
            .on_conflict_do_nothing()
            .returning(Lock.queue)
        ).scalar()
        if result is not None:
            new_queues.add(queue)

    if new_queues:
        log.info('scheduler started for queues=%s', ','.join(sorted(new_queues)))
    ctx._scheduler_queues.update(new_queues)

    stale_cutoff_time = now - ctx._timeout
    stale_schedules = (
        db.query(model.JobSchedule)
        .with_for_update()
        .filter(
            model.JobSchedule.queue.in_(ctx._scheduler_queues),
            model.JobSchedule.is_enabled == sa.true(),
            model.JobSchedule.next_execution_time < stale_cutoff_time,
        )
        .all()
    )
    for s in stale_schedules:
        next_execution_time = get_next_rrule_time(
            s.rrule, s.created_time, stale_cutoff_time)
        log.warning(
            'execution time on schedule=%s is very old (%s seconds), skipping '
            'to next time=%s',
            s.id, (now - s.next_execution_time).total_seconds(),
            next_execution_time,
        )
        s.next_execution_time = next_execution_time

    set_next_schedule_time(ctx, db=db, model=model)


@dbsession
def set_next_schedule_time(ctx, *, db, model):
    if not ctx._scheduler_queues:
        ctx._next_schedule_time = datetime.max
        return

    ctx._next_schedule_time = (
        db.query(sa.func.min(model.JobSchedule.next_execution_time))
        .filter(
            model.JobSchedule.queue.in_(ctx._scheduler_queues),
            model.JobSchedule.is_enabled == sa.true(),
        )
        .scalar()
    )

    if ctx._next_schedule_time is None:
        log.debug('no pending schedules')
        ctx._next_schedule_time = datetime.max

    else:
        when = (ctx._next_schedule_time - ctx._now()).total_seconds()
        if when > 0:
            log.debug('tracking next schedule in %.3f seconds', when)


@dbsession
def apply_schedules(ctx, *, now=None, db, model):
    if now is None:
        now = ctx._now()

    mq_source = MQSource(dbsession=db, model=model)
    schedules = (
        db.query(model.JobSchedule)
        .with_for_update(of=model.JobSchedule, skip_locked=True)
        .filter(
            model.JobSchedule.next_execution_time <= now,
            model.JobSchedule.queue.in_(ctx._scheduler_queues),
            model.JobSchedule.is_enabled == sa.true(),
        )
        .order_by(
            model.JobSchedule.next_execution_time.asc(),
            model.JobSchedule.created_time.asc(),
        )
        .all()
    )
    for schedule in schedules:
        mq_source.call_schedule(
            schedule.id,
            now=now,
            when=schedule.next_execution_time,
            reload=False,
        )

    if ctx._running:
        set_next_schedule_time(ctx, db=db, model=model)

        # refresh the job status because this thread will have missed the
        # notify from the new jobs
        set_next_job_time(ctx, db=db, model=model)


class ListenEventType:
    CONTINUE = 'continue'
    NOTIFY = 'notify'
    JOB_READY = 'job_ready'
    JOB_DONE = 'job_done'
    SCHEDULE_READY = 'schedule_ready'
    MAINTENANCE = 'maintenance'
    SHUTDOWN = 'shutdown'


class BoringEvent:
    def __init__(self, type):
        self.type = type


CONTINUE_EVENT = BoringEvent(ListenEventType.CONTINUE)
JOB_READY_EVENT = BoringEvent(ListenEventType.JOB_READY)
JOB_DONE_EVENT = BoringEvent(ListenEventType.JOB_DONE)
SCHEDULE_READY_EVENT = BoringEvent(ListenEventType.SCHEDULE_READY)
MAINTENANCE_EVENT = BoringEvent(ListenEventType.MAINTENANCE)
SHUTDOWN_EVENT = BoringEvent(ListenEventType.SHUTDOWN)


class NotifyEvent:
    type = ListenEventType.NOTIFY
    next_job_time = datetime.max
    next_schedule_time = datetime.max


def handle_notifies(ctx, conn):
    try:
        conn.poll()
    except psycopg2.OperationalError as ex:
        # this is a little hairy but since we are using the raw psycopg2
        # connection we cannot rely on sqlalchemy's nice error handling so
        # we test the connection directly to see if it's closed and if it is
        # we reconnect - we set the previous connection to None so that
        # rotate_dbconn doesn't invoke connection.close() on it
        if conn.closed:
            log.warning(
                'connection closed unexpectedly, error="%s"',
                str(ex).strip(),
            )
            ctx._dbconn = None
            rotate_dbconn(ctx)
            return CONTINUE_EVENT
        raise

    event = NotifyEvent()
    while conn.notifies:
        notify = conn.notifies.pop(0)
        channel, payload = notify.channel, notify.payload

        try:
            queue = channel[len(ctx._model.channel_prefix):]
            data = json.loads(payload)
            t = data['t']
            event_time = int_to_datetime(t)

            if 'j' in data:
                if event_time < event.next_job_time:
                    event = NotifyEvent()
                    event.next_job_time = event_time

            elif 's' in data:
                if (
                    queue in ctx._scheduler_queues
                    and event_time < event.next_schedule_time
                ):
                    event = NotifyEvent()
                    event.next_schedule_time = event_time

            else:
                raise Exception

        except Exception:
            log.exception('error while handling event from channel=%s, '
                          'payload=%s', channel, payload)
            continue

    return event


def get_next_event(ctx):
    conn = ctx._dbconn.connection

    rlist = [ctx._job_trigger]
    if ctx._running:
        rlist.append(conn)
        if ctx._shutdown_trigger is not None:
            rlist.append(ctx._shutdown_trigger)

    now = ctx._now()
    timeouts = [ctx._timeout]
    if ctx._running:
        timeouts.append(ctx._next_maintenance_time - now)
        timeouts.append(ctx._next_schedule_time - now)

        if len(ctx._active_jobs) < ctx._threads:
            timeouts.append(ctx._next_job_time - now)

    # add some jitter to the timeout if we are waiting on a future event
    # so that workers are staggered when they wakeup if they are all
    # waiting on the same event
    timeout = max(0, min(timeouts).total_seconds())
    timeout += random.uniform(0, ctx._jitter)

    log.debug('watching for events with timeout=%.3f', timeout)
    result, *_ = select.select(rlist, [], [], timeout)

    # always cleanup jobs, so keep this above the ctx._running check below
    if ctx._job_trigger in result:
        ctx._job_trigger.read()
        return JOB_DONE_EVENT

    # do not handle new jobs or maintenance while shutting down
    if not ctx._running:
        return CONTINUE_EVENT

    if conn in result:
        return handle_notifies(ctx, conn)

    now = ctx._now()
    if ctx._next_schedule_time <= now:
        return SCHEDULE_READY_EVENT

    if ctx._next_job_time <= now and len(ctx._active_jobs) < ctx._threads:
        return JOB_READY_EVENT

    if ctx._next_maintenance_time <= now:
        return MAINTENANCE_EVENT

    return CONTINUE_EVENT


def eventloop(ctx):
    while ctx._running or ctx._active_jobs:
        # wait for either _next_job_time timeout or a new event
        event = get_next_event(ctx)

        if event.type == ListenEventType.JOB_READY:
            enqueue_jobs(ctx)

        elif event.type == ListenEventType.JOB_DONE:
            finish_jobs(ctx)

        elif event.type == ListenEventType.SCHEDULE_READY:
            apply_schedules(ctx)

        elif event.type == ListenEventType.NOTIFY:
            if event.next_job_time < ctx._next_job_time:
                ctx._next_job_time = event.next_job_time
                when = (event.next_job_time - ctx._now()).total_seconds()
                log.debug('tracking next job in %.3f seconds', when)

            if event.next_schedule_time < ctx._next_schedule_time:
                ctx._next_schedule_time = event.next_schedule_time
                when = (event.next_schedule_time - ctx._now()).total_seconds()
                log.debug('tracking next schedule in %.3f seconds', when)

        elif event.type == ListenEventType.MAINTENANCE:
            run_maintenance(ctx)
