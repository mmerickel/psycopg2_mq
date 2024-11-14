===========
psycopg2_mq
===========

.. image:: https://img.shields.io/pypi/v/psycopg2_mq.svg
    :target: https://pypi.org/pypi/psycopg2_mq

.. image:: https://github.com/mmerickel/psycopg2_mq/actions/workflows/ci-tests.yml/badge.svg?branch=main
    :target: https://github.com/mmerickel/psycopg2_mq/actions/workflows/ci-tests.yml?query=branch%3Amain
    :alt: main CI Status

``psycopg2_mq`` is a message queue implemented on top of
`PostgreSQL <https://www.postgresql.org/>`__,
`SQLAlchemy <https://www.sqlalchemy.org/>`__, and
`psycopg2 <http://initd.org/psycopg/>`__.

Currently the library provides only the low-level constructs that can be used
to build a multithreaded worker system. It is broken into two components:

- ``psycopg2_mq.MQWorker`` - a reusable worker object that manages a
  single-threaded worker that can accept jobs and execute them. An application
  should create worker per thread. It supports an API for thread-safe graceful
  shutdown.

- ``psycopg2_mq.MQSource`` - a source object providing a client-side API for
  invoking and querying job states.

It is expected that these core components are then wrapped into your own
application in any way that you see fit, without dictating a specific CLI
or framework.

Data Model
==========

Queues
------

Workers run jobs defined in queues. Currently each queue will run jobs
concurrently, while a future version may support serial execution on a
per-queue basis. Each registered queue should contain an ``execute_job(job)``
method.

Jobs
----

The ``execute_job`` method of a queue is passed a ``Job`` object containing
the following attributes:

- ``id``
- ``queue``
- ``method``
- ``args``
- ``cursor``

As a convenience, there is an ``extend(**kw)`` method which can be used to
add extra attributes to the object. This is useful in individual queues to
define a contract between a queue and its methods.

Cursors
~~~~~~~

By defining a cursor you can execute jobs sequentially instead of in parallel.
There can only be one running (or lost) job for any cursor at a time.

A ``Job`` can be scheduled with a ``cursor_key``.

A ``job.cursor`` dict is provided to the workers containing the cursor data,
and is saved back to the database when the job is completed. This effectively
gives jobs some persistent, shared memory between jobs on the cursor.

Because a "lost" job on the cursor counts as running, it will also block any other jobs from executing.
To resolve this situation it is necessary for the lost job to be either marked as failed or retried.
Look at ``MQSource.retry_job`` and ``MQSource.fail_lost_job`` APIs.

Collapsible
~~~~~~~~~~~

When ``collapse_on_cursor`` is set to ``True`` on a job, this is declaring the job as "collapsible".
There can only be one job in the "pending" state marked ``collapsible == True`` for the same ``queue``, ``method``, and ``cursor_key``.
This means that if there is an existing "pending" job with the same ``cursor_key``, ``queue``, and ``method`` with ``collapsible == True`` then the new job will be "collapsed" into the existing job.
By default, this typically means that the new job will be dropped because another job already exists.
This means that the ``args`` should be "constant", because the new job's args are ignored.

If the ``args`` are not constant, then you will likely want to pass a ``conflict_resolver`` callback.
This is a function that will be invoked when a job already exists.
You can then update the existing ``Job`` object in the "pending" state, adjusting the args etc.

Delayed Jobs
~~~~~~~~~~~~

A ``Job`` can be delayed to run in the future by providing a ``datetime`` object to the ``when`` argument.
When this feature is used with collapsible jobs you can create a highly efficient throttle greatly reducing the number of jobs that run in the background.
For example, schedule 20 jobs all with the same ``cursor_key``, and ``collapse_on_cursor=True``, and ``when=timedelta(seconds=30)``.
You will see only 1 job created and it will execute in 30 seconds.
All of the other jobs are collapsed into this one instead of creating separate jobs.

Schedules
---------

A ``JobSchedule`` can be defined which supports
`RFC 5545 <https://tools.ietf.org/html/rfc5545>`__ ``RRULE`` schedules. These
are powerful and can support timezones, frequencies based on calendars as well
as simple recurring rules from an anchor time using ``DTSTART``. Cron jobs
can be converted to this syntax for simpler scenarios.

``psycopg2-mq`` workers will automatically negotiate which worker is responsible
for managing schedules so clustered workers should operate as expected.

To register a new schedule, look at the ``MQSource.add_schedule(queue, method, args, *, rrule)`` API.

If you set ``collapse_on_cursor`` is ``True`` on the schedule and there is already a job pending then the firing of the schedule is effectively a no-op.

Events / Listeners
------------------

A ``JobListener`` can be defined which supports creating new jobs when events are
emitted. When an event is emitted via ``MQSource.emit_event`` then any listeners
matching this event will be used to create a new job in the system.

To register a listener, look at the ``MQSource.add_listener(event, queue, method, args, ...)`` API.

There is a default event emitted every time a job moved to a finished state. It has the format::

  mq.job_finished.<state><queue>.<method>

For example, ``mq.job_finished.completed.dummy.echo``.

You are free to emit your own custom events as well if you need different dimensions!

When ``collapse_on_cursor`` is ``False`` then the listener receives an ``event`` arg containing the ``name``, ``listener_id``, and ``data`` keys.

If ``collapse_on_cursor`` is ``True`` on the listener then the resulting job will receive an ``events`` arg containing a list of all of the emitted events that occured while the job was in the "pending" state.

Example Worker
==============

.. code-block:: python

    from psycopg2_mq import (
        MQWorker,
        make_default_model,
    )
    from sqlalchemy import (
        MetaData,
        create_engine,
    )
    import sys

    class EchoQueue:
        def execute_job(self, job):
            return f'hello, {job.args["name"]} from method="{job.method}"'

    if __name__ == '__main__':
        engine = create_engine(sys.argv[1])
        metadata = MetaData()
        model = make_default_model(metadata)
        worker = MQWorker(
            engine=engine,
            queues={
                'echo': EchoQueue(),
            },
            model=model,
        )
        worker.run()

Example Source
==============

.. code-block:: python

    engine = create_engine('postgresql+psycopg2://...')
    metadata = MetaData()
    model = make_default_model(metadata)
    metadata.create_all(engine)
    session_factory = sessionmaker(engine)

    with session_factory.begin():
        mq = MQSource(
            dbsession=dbsession,
            model=model,
        )
        job_id = mq.call('echo', 'hello', {'name': 'Andy'})
        print(f'queued job={job_id}')
