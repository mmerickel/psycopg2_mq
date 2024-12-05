Changes
=======

0.12.7 (2024-12-05)
-------------------

- Fixed an issue when claiming jobs if the SQLAlchemy Job model passed in to MQModel is
  not named ``Job`` in the SQLAlchemy metadata.

0.12.6 (2024-11-14)
-------------------

- Fix linting.

0.12.5 (2024-11-14)
-------------------

- Change the format to put the state in the middle of the emitted event.

- Emit an event when moving from lost to failed via ``MQSource.fail_lost_job``.

- Add the ``queue`` and ``method`` to the event args on ``mq.job_finished`` events

0.12.4 (2024-11-13)
-------------------

- Emit an event any time a job hits a terminal state. The format has changed from
  ``mq_job_complete.<queue>.<method>`` to ``mq.job_finished.<queue>.<method>.<state>``.
  This includes ``completed``, ``failed``, and ``lost``.

0.12.3 (2024-10-29)
-------------------

- Add ``job.schedule_ids`` and ``job.listener_ids`` to the job context passed to the
  background workers.

0.12.2 (2024-10-29)
-------------------

- Prevent canceling jobs in the lost state.

- Allow retrying jobs in any state.

- Allow setting a result on a lost job when moving it to failed.

0.12.1 (2024-10-27)
-------------------

- Support SQLAlchemy 2.x.

- Must mark a lost job as failed prior to being able to retry it.

0.12 (2024-10-27)
-----------------

- Support a job being linked properly to multiple schedule and listener sources such
  that provenance is properly tracked on retries.

- Add a new ``CANCELED`` job state that can be used to manually mark any pending,
  failed, or lost jobs as canceled. Jobs do not enter this state automatically - theyt
  must be manually marked but will be useful to disambiguate failed from canceled.

- [breaking] ``job.schedule_id`` is removed from the job object passed to background
  workers.

- [model migration] Moved the ``schedule_id`` and ``listener_id`` foreign keys from
  the ``Job`` table to many-to-many link tables to support tracking the source properly
  when collapsing occurs. Possible migration::

    insert into mq_job_schedule_link (job_id, schedule_id)
      select id, schedule_id from mq_job where schedule_id is not null;

    insert into mq_job_listener_link (job_id, listener_id)
      select id, listener_id from mq_job where listener_id is not null;

    alter table mq_job drop column schedule_id;
    alter table mq_job drop column listener_id;

- [model migration] Add a new ``CANCELED`` state to the ``mq_job_state`` enum.
  Possible migration::

    alter type mq_job_state add value 'canceled';

0.11 (2024-10-27)
-----------------

- Add support for Python 3.13.

- [breaking] Modified the ``MQSource.call``, and ``MQSource.add_schedule`` APIs such
  that when a cursor is used ``collapse_on_cursor`` defaults to ``False`` instead of
  ``True``. You must explicitly set it to ``True`` in scenarios in which that is
  desired as it is no longer the default behavior.

- [model migration] Add ``collapse_on_cursor`` attribute to
  the ``JobSchedule`` model. A bw-compat migration would set this value to ``False``
  if ``cursor_key`` is ``NULL`` and ``True`` on everything else.

- [model migration] Add a new ``JobListener`` model.

- [model migration] Add ``listener_id`` foreign key to the ``Job`` model.

- Fix a bug in which NOTIFY events were missed in some cases causing jobs to wait
  until the maintenance window to execute.

- Add the concept of pub/sub event listeners. Listeners can be registered that act as a
  job factory, creating a new job when an event is emitted.

  It is possible to emit events manually as needed via the ``MQSource.emit_event`` API.

  Events are emitted automatically when a job is completed. Every job when it is
  completed successfully emits a new ``mq_job_complete:<queue>.<method>`` event.
  This event contains the result of the job.

- The ``MQSource`` that is used by the ``MQWorker`` can now be overridden via the
  ``mq_source_factory`` option.

0.10 (2024-08-06)
------------------

- Add support for Python 3.12.

- Drop support for Python 3.7, and 3.8.

- Fix a race condition on shutdown where the job fails to cleanup because the triggers
  are gone while the pool is still shutting down.

0.9 (2023-04-21)
----------------

- Add support for Python 3.10, and 3.11.

- [breaking] Prevent retrying of collapsible jobs. Require them to be invoked
  using ``call`` instead for an opportunity to specify a ``conflict_resolver``.

- [model migration] Fix a bug in the default model schema in which the
  collapsible database index was not marked unique.

- Copy trace info when retrying a job.

- Capture the stringified exception to the job result in the ``message`` key,
  alongside the existing ``tb``, ``exc``, and ``args`` keys.

- The worker was not recognizing ``capture_signals=False``, causing problems
  when running the event loop in other threads.

- Blackify the codebase and add some real tests. Yay!

0.8.3 (2022-04-15)
------------------

- [breaking] Remove ``MQWorker.make_job_context``.

0.8.2 (2022-04-15)
------------------

- Drop Python 3.6 support.

- [breaking] Require SQLAlchemy 1.4+ and resolve deprecation warnings related to
  SQLAlchemy 2.0.

- [model migration] Rename ``update_job_id`` to ``updated_job_id`` in the
  ``JobCursor`` model.

0.8.1 (2022-04-15)
------------------

- Ensure the ``trace`` attribute is populated on the ``JobContext``.

- Add ``MQWorker.make_job_context`` which can be defined to completely override
  the ``JobContext`` factory using the ``Job`` object and open database session.

0.8.0 (2022-04-15)
------------------

- [model migration] Add ``update_job_id`` foreign key to the ``JobCursor`` model to
  make it possible to know which job last updated the value in the cursor.

- [model migration] Add ``trace`` json blob to the ``Job`` model.

- Support a ``trace`` json blob when creating new jobs. This value is available
  on the running job context and can be used when creating sub-jobs or when
  making requests to external systems to pass through tracing metadata.

  See ``MQSource.call``'s new ``trace`` parameter when creating jobs.
  See ``JobContext.trace`` attribute when handling jobs.

- Add a standard ``FailedJobError`` exception which can be raised by jobs to
  mark a failure with a custom result object. This is different from unhandled
  exceptions that cause the ``MQWorker.result_from_error`` method to be invoked.

0.7.0 (2022-03-03)
------------------

- Fix a corner case with lost jobs attached to cursors. In scenarios where
  multiple workers are running, if one loses a database connection then the
  other is designed to notice and mark jobs lost. However, it's possible the
  job is not actually lost and the worker can then recover after resuming
  its connection, and marking the job running again. In this situation, we
  do not want another job to begin on the same cursor. To fix this issue,
  new jobs will not be run if another job is marked lost on the same cursor.
  You will be required to recover the job by marking it as not lost (probably
  failed) first to unblock the rest of the jobs on the cursor.

0.6.2 (2022-03-01)
------------------

- Prioritize maintenance work higher than running new jobs.
  There was a chicken-and-egg issue where a job would be marked running
  but needs to be marked lost. However marking it lost is lower priority than
  trying to start new jobs. In the case where a lot of jobs were scheduled
  at the same time, the worker always tried to start new jobs and didn't
  run the maintenance so the job never got marked lost, effectively blocking
  the queue.

0.6.1 (2022-01-15)
------------------

- Fix a bug introduced in the 0.6.0 release when scheduling new jobs.

0.6.0 (2022-01-14)
------------------

- [model migration] Add model changes to mark jobs as collapsible.

- [model migration] Add model changes to the cursor index.

- Allow multiple pending jobs to be scheduled on the same cursor if either:

  1. The queue or method are different from existing pending jobs on the cursor.

  2. ``collapse_on_cursor`` is set to ``False`` when scheduling the job.

0.5.7 (2021-03-07)
------------------

- Add a ``schedule_id`` attribute to the job context for use in jobs that want
  to know whether they were executed from a schedule or not.

0.5.6 (2021-02-28)
------------------

- Some UnicodeDecodeError exceptions raised from jobs could trigger a
  serialization failure (UntranslatableCharacter) because it would contain
  the sequence ``\u0000``` which, while valid in Python, is not allowed
  in postgres. So when dealing with the raw bytes, we'll decode it with
  the replacement character that can be properly stored. Not ideal, but
  better than failing to store the error at all.

0.5.5 (2021-01-22)
------------------

- Fixed some old code causing the worker lock to release after a job
  completed.

0.5.4 (2021-01-20)
------------------

- Log at the error level when marking a job as lost.

0.5.3 (2021-01-11)
------------------

- Copy the ``schedule_id`` information to retried jobs.

0.5.2 (2021-01-11)
------------------

- [breaking] Require ``call_schedule`` to accept an id instead of an object.

0.5.1 (2021-01-09)
------------------

- [model migration] Drop the ``UNIQUE`` constraint on the background job
  ``lock_id`` column.

0.5 (2021-01-09)
----------------

- [model migration] Add a scheduler model with support for emitting periodic
  jobs based on RRULE syntax.
  See https://github.com/mmerickel/psycopg2_mq/pull/11

- Enable the workers to coordinate on a per-queue basis who is in control
  of scheduling jobs.
  See https://github.com/mmerickel/psycopg2_mq/pull/12

- Reduce the number of advisory locks held from one per job to one per worker.
  See https://github.com/mmerickel/psycopg2_mq/pull/12

0.4.5 (2020-12-22)
------------------

- Use column objects in the insert statement to support ORM-level synonyms,
  enabling the schema to have columns with different names.

0.4.4 (2019-11-07)
------------------

- Ensure the advisory locks are released when a job completes.

0.4.3 (2019-10-31)
------------------

- Ensure maintenance (finding lost jobs) always runs at set intervals defined
  by the ``timeout`` parameter.

0.4.2 (2019-10-30)
------------------

- Recover active jobs when the connection is lost by re-locking them
  and ensuring they are marked running.

0.4.1 (2019-10-30)
------------------

- Attempt to reconnect to the database after losing the connection.
  If the reconnect attempt fails then crash.

0.4 (2019-10-28)
----------------

- [model migration] Add a ``worker`` column to the ``Job`` model to track what
  worker is handling a job.

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
------------------

- Only save a cursor update if the job is completed successfully.

0.3.2 (2019-10-22)
------------------

- Mark lost jobs during timeouts instead of just when a worker starts in order
  to catch them earlier.

0.3.1 (2019-10-17)
------------------

- When attempting to schedule a job with a cursor and a ``scheduled_time``
  earlier than a pending job on the same cursor, the job will be updated to
  run at the earlier time.

- When attempting to schedule a job with a cursor and a pending job already
  exists on the same cursor, a ``conflict_resolver`` function may be
  supplied to ``MQSource.call`` to update the job properties, merging the
  arguments however the user wishes.

0.3 (2019-10-15)
----------------

- [model migration] Add a new column ``cursor_snapshot`` to the ``Job`` model which
  will contain the value of the cursor when the job begins.

0.2 (2019-10-09)
----------------

- [model migration] Add cursor support for jobs. This requires a schema migration to
  add a ``cursor_key`` column, a new ``JobCursor`` model, and some new indices.

0.1.6 (2019-10-07)
------------------

- Support passing custom kwargs to the job in ``psycopg2_mq.MQSource.call``
  to allow custom columns on the job table.

0.1.5 (2019-05-17)
------------------

- Fix a regression when serializing errors with strings or cycles.

0.1.4 (2019-05-09)
------------------

- More safely serialize exception objects when jobs fail.

0.1.3 (2018-09-04)
------------------

- Rename the thread to contain the job id while it's handling a job.

0.1.2 (2018-09-04)
------------------

- [model migration] Rename ``Job.params`` to ``Job.args``.

0.1.1 (2018-09-04)
------------------

- Make ``psycopg2`` an optional dependency in order to allow apps to depend
  on ``psycopg2-binary`` if they wish.

0.1 (2018-09-04)
----------------

- Initial release.
