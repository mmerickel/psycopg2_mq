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
