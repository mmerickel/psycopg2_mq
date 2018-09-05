===========
psycopg2_mq
===========

.. image:: https://img.shields.io/pypi/v/psycopg2_mq.svg
    :target: https://pypi.org/pypi/psycopg2_mq

.. image:: https://img.shields.io/travis/mmerickel/psycopg2_mq/master.svg
    :target: https://travis-ci.org/mmerickel/psycopg2_mq

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

As a convenience, there is an ``extend(**kw)`` method which can be used to
add extra attributes to the object. This is useful in individual queues to
define a contract between a queue and its methods.

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

    engine = create_engine()
    metadata = MetaData()
    model = make_default_model(metadata)
    session_factory = sessionmaker()
    session_factory.configure(bind=engine)

    dbsession = session_factory()
    with dbsession.begin():
      mq = MQSource(
          dbsession=dbsession,
          model=model,
      )
      job = mq.call('echo', 'hello', {'name': 'Andy'})
      print(f'queued job={job.id}')
