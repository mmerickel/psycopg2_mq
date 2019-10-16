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
