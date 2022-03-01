0.6.2 (2022-03-01)
==================

- Prioritize maintenance work higher than running new jobs.
  There was a chicken-and-egg issue where a job would be marked running
  but needs to be marked lost. However marking it lost is lower priority than
  trying to start new jobs. In the case where a lot of jobs were scheduled
  at the same time, the worker always tried to start new jobs and didn't
  run the maintenance so the job never got marked lost, effectively blocking
  the queue.

0.6.1 (2022-01-15)
==================

- Fix a bug introduced in the 0.6.0 release when scheduling new jobs.

0.6.0 (2022-01-14)
==================

- [breaking] Add model changes to mark jobs as collapsible.

- [breaking] Add model changes to the cursor index.

- Allow multiple pending jobs to be scheduled on the same cursor if either:

  1. The queue or method are different from existing pending jobs on the cursor.

  2. ``collapse_on_cursor`` is set to ``False`` when scheduling the job.

0.5.7 (2021-03-07)
==================

- Add a ``schedule_id`` attribute to the job context for use in jobs that want
  to know whether they were executed from a schedule or not.

0.5.6 (2021-02-28)
==================

- Some UnicodeDecodeError exceptions raised from jobs could trigger a
  serialization failure (UntranslatableCharacter) because it would contain
  the sequence ``\u0000``` which, while valid in Python, is not allowed
  in postgres. So when dealing with the raw bytes, we'll decode it with
  the replacement character that can be properly stored. Not ideal, but
  better than failing to store the error at all.

0.5.5 (2021-01-22)
==================

- Fixed some old code causing the worker lock to release after a job
  completed.

0.5.4 (2021-01-20)
==================

- Log at the error level when marking a job as lost.

0.5.3 (2021-01-11)
==================

- Copy the ``schedule_id`` information to retried jobs.

0.5.2 (2021-01-11)
==================

- [breaking] Require ``call_schedule`` to accept an id instead of an object.

0.5.1 (2021-01-09)
==================

- Drop the ``UNIQUE`` constraint on the background job ``lock_id`` column.

0.5 (2021-01-09)
================

- Add a scheduler model with support for emitting periodic jobs based on
  RRULE syntax.
  See https://github.com/mmerickel/psycopg2_mq/pull/11

- Enable the workers to coordinate on a per-queue basis who is in control
  of scheduling jobs.
  See https://github.com/mmerickel/psycopg2_mq/pull/12

- Reduce the number of advisory locks held from one per job to one per worker.
  See https://github.com/mmerickel/psycopg2_mq/pull/12

0.4.5 (2020-12-22)
==================

- Use column objects in the insert statement to support ORM-level synonyms,
  enabling the schema to have columns with different names.

0.4.4 (2019-11-07)
==================

- Ensure the advisory locks are released when a job completes.

0.4.3 (2019-10-31)
==================

- Ensure maintenance (finding lost jobs) always runs at set intervals defined
  by the ``timeout`` parameter.

0.4.2 (2019-10-30)
==================

- Recover active jobs when the connection is lost by re-locking them
  and ensuring they are marked running.

0.4.1 (2019-10-30)
==================

- Attempt to reconnect to the database after losing the connection.
  If the reconnect attempt fails then crash.

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
