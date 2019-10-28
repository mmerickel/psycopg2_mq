from concurrent import futures
from contextlib import contextmanager
from datetime import datetime
import json
import os
import platform
import random
import select
import signal
import sqlalchemy as sa
from sqlalchemy.orm import Session
from sqlalchemy.sql import table, column
import sys
import threading
import traceback

from .trigger import Trigger
from .util import (
    clamp,
    class_name,
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


class ExitRequest(Exception):
    """ Raised to abort the event loop."""


class JobContext:
    def __init__(self, id, queue, method, args, cursor=None):
        self.id = id
        self.queue = queue
        self.method = method
        self.args = args
        self.cursor = cursor

    def extend(self, **kw):
        for k, v in kw.items():
            setattr(self, k, v)


class MQWorker:
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

        self._timeout = timeout
        self._jitter = jitter
        self._lock_key = lock_key
        self._threads = threads
        self._capture_signals = capture_signals

        if name is None:
            name = guess_worker_name()
        self._name = name

        self._running = False

        self._dbconn = None
        self._shutdown_trigger = None
        self._job_trigger = None
        self._next_date = datetime.max
        self._pool = None
        self._active_jobs = {}

    def shutdown_gracefully(self):
        self._running = False
        if self._shutdown_trigger:
            self._shutdown_trigger.notify()

    def run(self):
        self._running = True
        try:
            log.info(f'starting mq worker={self._name}')
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
        return {
            'name': self._name,
            'running': self._running,
            'total_threads': self._threads,
            'idle_threads': self._threads - len(self._active_jobs),
            'active_jobs': [
                {
                    'id': j.id,
                    'queue': j.queue,
                    'method': j.method,
                    'args': j.args,
                }
                for j in self._active_jobs.values()
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
    with ctx._engine.connect() as conn:
        conn.detach()
        conn.execution_options(autocommit=True)
        assert conn.dialect.driver == 'psycopg2'
        curs = conn.connection.cursor()
        for channel in ctx._queues.keys():
            curs.execute('LISTEN %s;' % channel)
        ctx._dbconn = conn
        try:
            yield

        finally:
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


@contextmanager
def dbsession(ctx):
    with ctx._dbconn.begin():
        db = Session(bind=ctx._dbconn)
        try:
            yield (db, ctx._model)
            db.commit()
        finally:
            db.close()


def get_lock_id(db, key, job, attempts=3):
    while attempts > 0:
        lock_id = random.randint(1, 2**31 - 1)
        is_locked = db.execute(
            'select pg_try_advisory_lock(:key, :id)',
            {'key': key, 'id': lock_id},
        ).scalar()
        if is_locked:
            return lock_id

        attempts -= 1
    raise RuntimeError(f'failed to get a unique lock_id for job={job.id}')


def claim_pending_job(ctx, now=None):
    if now is None:
        now = datetime.utcnow()

    with dbsession(ctx) as (db, model):
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

            job.lock_id = get_lock_id(db, ctx._lock_key, job)
            job.state = model.JobStates.RUNNING
            job.start_time = datetime.utcnow()
            job.worker = ctx._name

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
                cursor=cursor,
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


def finish_job(ctx, job_id, success, result, cursor=None):
    with dbsession(ctx) as (db, model):
        state = (
            model.JobStates.COMPLETED
            if success
            else model.JobStates.FAILED
        )
        job = (
            db.query(model.Job)
            .with_for_update()
            .filter_by(id=job_id)
            .one()
        )

        job.state = state
        job.result = result
        job.end_time = datetime.utcnow()
        job.lock_id = None

        if success:
            if job.cursor_key is not None:
                save_cursor(db, model, job, cursor)

            elif cursor is not None:
                log.warn('ignoring cursor for job=%s without a cursor_key', job_id)

        db.flush()
        db.execute(
            'select pg_advisory_unlock(:key, :id)',
            {'key': ctx._lock_key, 'id': job.lock_id},
        )

    log.info('finished processing job=%s, state="%s"', job_id, state)


def save_cursor(db, model, job, cursor):
    if cursor is None:
        cursor = {}
    cursor_obj = (
        db.query(model.JobCursor)
        .filter(model.JobCursor.key == job.cursor_key)
        .with_for_update()
        .first()
    )
    if cursor_obj is None:
        cursor_obj = model.JobCursor(
            key=job.cursor_key,
            properties=cursor,
        )
        db.add(cursor_obj)

    elif cursor_obj.properties != cursor:
        cursor_obj.properties = cursor


def mark_lost_jobs(ctx):
    with dbsession(ctx) as (db, model):
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
            log.info('marking job=%s as lost', job.id)


def set_next_date(ctx):
    with dbsession(ctx) as (db, model):
        ctx._next_date = (
            db.query(sa.func.min(model.Job.scheduled_time))
            .filter(
                model.Job.state == model.JobStates.PENDING,
                model.Job.queue.in_(ctx._queues.keys()),
            )
            .scalar()
        )

    if ctx._next_date is None:
        log.debug('no pending jobs')
        ctx._next_date = datetime.max

    else:
        when = (ctx._next_date - datetime.utcnow()).total_seconds()
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
        set_next_date(ctx)


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


class ListenEventType:
    CONTINUE = 'continue'
    JOB_READY = 'job_ready'
    JOB_DONE = 'job_done'
    NEW_JOB = 'new_job'
    TIMEOUT = 'timeout'
    SHUTDOWN = 'shutdown'


class BoringEvent:
    def __init__(self, type):
        self.type = type


CONTINUE_EVENT = BoringEvent(ListenEventType.CONTINUE)
JOB_READY_EVENT = BoringEvent(ListenEventType.JOB_READY)
JOB_DONE_EVENT = BoringEvent(ListenEventType.JOB_DONE)
TIMEOUT_EVENT = BoringEvent(ListenEventType.TIMEOUT)
SHUTDOWN_EVENT = BoringEvent(ListenEventType.SHUTDOWN)


class NewJobEvent:
    type = ListenEventType.NEW_JOB

    def __init__(self, *, job_time, job_queue, job_id):
        self.job_id = job_id
        self.job_time = job_time
        self.job_queue = job_queue


def get_next_event(ctx):
    conn = ctx._dbconn.connection

    rlist = [ctx._job_trigger]
    if ctx._running:
        rlist.append(conn)
        if ctx._shutdown_trigger is not None:
            rlist.append(ctx._shutdown_trigger)

    # if not saturated then reduce the timeout to the next job start time
    if ctx._running and len(ctx._active_jobs) < ctx._threads:
        timeout = (ctx._next_date - datetime.utcnow()).total_seconds()
    else:
        timeout = ctx._timeout

    # add some jitter to the timeout if we are waiting on a future event
    # so that workers are staggered when they wakeup if they are all
    # waiting on the same event
    timeout = clamp(timeout, 0, ctx._timeout)
    timeout += random.uniform(0, ctx._jitter)

    log.debug('watching for events with timeout=%.3f', timeout)
    result, *_ = select.select(rlist, [], [], timeout)

    # always cleanup jobs, so keep this above the ctx._running check below
    if ctx._job_trigger in result:
        ctx._job_trigger.read()
        return JOB_DONE_EVENT

    # do not handle new jobs while shutting down
    if not ctx._running:
        return TIMEOUT_EVENT

    if conn in result:
        conn.poll()
        event = None
        while conn.notifies:
            notify = conn.notifies.pop(0)
            queue, payload = notify.channel, notify.payload

            try:
                payload = json.loads(payload)
                job_id, t = payload['j'], payload['t']
                job_time = int_to_datetime(t)

            except Exception:
                log.exception('error while handling event from channel=%s, '
                              'payload=%s', queue, payload)

            if event is None or job_time < event.job_time:
                event = NewJobEvent(
                    job_queue=queue,
                    job_id=job_id,
                    job_time=job_time,
                )
        return event or CONTINUE_EVENT

    if ctx._next_date <= datetime.utcnow():
        return JOB_READY_EVENT

    return TIMEOUT_EVENT


def eventloop(ctx):
    mark_lost_jobs(ctx)
    enqueue_jobs(ctx)
    while ctx._running or ctx._active_jobs:
        # wait for either _next_date timeout or a new event
        event = get_next_event(ctx)

        if event.type == ListenEventType.JOB_READY:
            enqueue_jobs(ctx)

        elif event.type == ListenEventType.JOB_DONE:
            finish_jobs(ctx)

        elif event.type == ListenEventType.NEW_JOB:
            if event.job_time < ctx._next_date:
                ctx._next_date = event.job_time
                when = (event.job_time - datetime.utcnow()).total_seconds()
                log.debug('tracking next job in %.3f seconds', when)

        elif event.type == ListenEventType.TIMEOUT:
            mark_lost_jobs(ctx)
            set_next_date(ctx)
