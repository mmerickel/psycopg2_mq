0.4 (2019-10-28)
================

- Add a ``worker`` column to the ``Job`` model to track what worker
  is handling a job.

- Add an optional ``name`` argument to ``MQWorker`` to name the worker -
  the value will be recorded in each job.

- Add a ``threads`` argument (default=``1``) to ``MQWorker`` to support
  handling multiple jobs from the same worker instance instead of making a
  worker per thread.

- Add ``capture_signals`` argument (default=``True``) to ``MQWorker`` which
  will capture ``SIGTERM``, ``SIGINT`` and ``SIGUSR1``. The first two will
  trigger graceful shutdown - they will make the process stop handling new
  jobs while finishing active jobs. The latter will dump to ``stderr`` a
  JSON dump of the current status of the worker.

0.3.3 (2019-10-23)
==================

- Only save a cursor update if the job is completed successfully.

0.3.2 (2019-10-22)
==================

- Mark lost jobs during timeouts instead of just when a worker starts in order
  to catch them earlier.

0.3.1 (2019-10-17)
==================

- When attempting to schedule a job with a cursor and a ``scheduled_time``
  earlier than a pending job on the same cursor, the job will be updated to
  run at the earlier time.

- When attempting to schedule a job with a cursor and a pending job already
  exists on the same cursor, a ``conflict_resolver`` function may be
  supplied to ``MQSource.call`` to update the job properties, merging the
  arguments however the user wishes.

0.3 (2019-10-15)
================

- Add a new column ``cursor_snapshot`` to the ``Job`` model which will
  contain the value of the cursor when the job begins.

0.2 (2019-10-09)
================

- Add cursor support for jobs. This requires a schema migration to add
  a ``cursor_key`` column, a new ``JobCursor`` model, and some new indices.

0.1.6 (2019-10-07)
==================

- Support passing custom kwargs to the job in ``psycopg2_mq.MQSource.call``
  to allow custom columns on the job table.

0.1.5 (2019-05-17)
==================

- Fix a regression when serializing errors with strings or cycles.

0.1.4 (2019-05-09)
==================

- More safely serialize exception objects when jobs fail.

0.1.3 (2018-09-04)
==================

- Rename the thread to contain the job id while it's handling a job.

0.1.2 (2018-09-04)
==================

- Rename ``Job.params`` to ``Job.args``.

0.1.1 (2018-09-04)
==================

- Make ``psycopg2`` an optional dependency in order to allow apps to depend
  on ``psycopg2-binary`` if they wish.

0.1 (2018-09-04)
================

- Initial release.
