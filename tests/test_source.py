from datetime import datetime

from psycopg2_mq import MQSource


def test_simple_call(model, dbsession):
    source = MQSource(dbsession=dbsession, model=model)
    with dbsession.begin():
        job_id = source.call("dummy", "echo", {"message": "hello world"})

    jobs = source.query_jobs.all()
    assert len(jobs) == 1
    job = jobs[0]
    assert job.queue == "dummy"
    assert job.method == "echo"
    assert job.args == {"message": "hello world"}

    job2 = source.get_job(job_id)
    assert job is job2


def test_call_collapses_on_cursor(model, dbsession):
    source = MQSource(dbsession=dbsession, model=model)
    with dbsession.begin():
        job_id = source.call(
            "dummy",
            "echo",
            {"message": "hello world original"},
            cursor_key="foo",
            collapse_on_cursor=True,
        )

    with dbsession.begin():
        job2_id = source.call(
            "dummy",
            "echo",
            {"message": "hello world again"},
            cursor_key="foo",
            collapse_on_cursor=True,
        )

    assert job_id == job2_id

    job = source.get_job(job_id)
    assert job.args == {"message": "hello world original"}


def test_retry_failed_job(model, dbsession):
    source = MQSource(dbsession=dbsession, model=model)
    with dbsession.begin():
        job_id = source.call(
            "dummy",
            "echo",
            {"message": "hello world"},
            cursor_key="foo",
            collapse_on_cursor=True,
        )
        job = source.get_job(job_id)
        job.state = model.JobStates.FAILED
        job.start_time = datetime.utcnow()
        job.end_time = datetime.utcnow()

    job2_id = source.retry_job(job_id)
    assert job2_id != job_id

    job = source.get_job(job_id)
    assert job.state == model.JobStates.FAILED

    job = source.get_job(job2_id)
    assert job.state == model.JobStates.PENDING
    assert job.queue == "dummy"
    assert job.method == "echo"
    assert job.cursor_key == 'foo'
    assert not job.collapsible
    assert job.args == {"message": "hello world"}
