import argparse
import logging
import sqlalchemy as sa
import sqlalchemy.orm
import threading
import time

from psycopg2_mq import MQSource, make_default_model

metadata = sa.MetaData()
model = make_default_model(metadata)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--url', required=True)
    parser.add_argument('--no-wait', action='store_true')
    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG)

    url = sa.engine.make_url(args.url)
    url = url.update_query_dict(
        {
            'options': '-c deadlock_timeout=30000',
        }
    )
    dbengine = sa.create_engine(url)

    cv = threading.Condition()  # request for step to start
    current_step = 0
    pids = [None, None]
    results = [False, False]

    def wait_for_step(target_step):
        with cv:
            while current_step != target_step:
                cv.wait()

    def increment_step():
        nonlocal current_step
        with cv:
            current_step += 1
            cv.notify_all()

    def th1_work():
        with sa.orm.Session(dbengine) as session:
            pids[0] = session.execute(sa.text('select pg_backend_pid()')).scalar()
            source = MQSource(dbsession=session, model=model)
            wait_for_step(1)
            source.call(
                "dummy",
                "echo",
                {"step": 1},
                cursor_key="foo-1",
                collapse_on_cursor=True,
            )
            increment_step()
            wait_for_step(3)
            # this will block so the main thread will wait until we see that happening
            # in pg_stat_activity before triggering step 4
            source.call(
                "dummy",
                "echo",
                {"step": 3},
                cursor_key="foo-2",
                collapse_on_cursor=True,
            )
        results[0] = True

    def th2_work():
        with sa.orm.Session(dbengine) as session:
            pids[1] = session.execute(sa.text('select pg_backend_pid()')).scalar()
            source = MQSource(dbsession=session, model=model)
            wait_for_step(2)
            source.call(
                "dummy",
                "echo",
                {"step": 2},
                cursor_key="foo-2",
                collapse_on_cursor=True,
            )
            increment_step()
            wait_for_step(4)
            source.call(
                "dummy",
                "echo",
                {"step": 4},
                cursor_key="foo-1",
                collapse_on_cursor=True,
            )
        results[1] = True

    th1 = threading.Thread(target=th1_work, daemon=True)
    th2 = threading.Thread(target=th2_work, daemon=True)

    th1.start()
    th2.start()

    increment_step()
    wait_for_step(3)
    # wait until step 3 is blocked on insert before we trigger step 4
    with sa.orm.Session(dbengine) as session:
        while True:
            if (
                session.execute(
                    sa.text(
                        """
                        select count(*)
                        from pg_catalog.pg_stat_activity a
                        join pg_catalog.pg_locks l
                            on a.pid = l.pid
                        where
                            a.pid = :th1_pid
                            and not l.granted
                        """
                    ),
                    {"th1_pid": pids[0]},
                ).scalar()
                > 0
            ):
                break
            time.sleep(0.1)

    # continue to step 4 and see if we deadlock
    increment_step()

    th1.join()
    th2.join()

    if not all(results):  # pragma: no cover
        raise AssertionError('deadlock occurred')


if __name__ == '__main__':
    raise SystemExit(main())
