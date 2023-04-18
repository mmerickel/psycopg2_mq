from datetime import datetime
import pytest

from psycopg2_mq import MQSource


def test_simple_call(model, dbsession):
    source = MQSource(dbsession=dbsession, model=model)
    job_id = source.call('dummy', 'echo', {'message': 'hello world'})
    dbsession.commit()

    jobs = dbsession.query(model.Job).all()
    assert len(jobs) == 1
    job = jobs[0]
    assert job.queue == 'dummy'
    assert job.method == 'echo'
    assert job.args == {'message': 'hello world'}

    job2 = source.find_job(job_id)
    assert job is job2


def test_call_collapses_on_cursor(model, dbsession):
    source = MQSource(dbsession=dbsession, model=model)
    job_id = source.call(
        'dummy', 'echo', {'message': 'hello world original'}, cursor_key='foo'
    )
    dbsession.commit()

    job2_id = source.call(
        'dummy', 'echo', {'message': 'hello world again'}, cursor_key='foo'
    )
    dbsession.commit()

    assert job_id == job2_id

    job = source.find_job(job_id)
    assert job.args == {'message': 'hello world original'}


def test_retry_error(model, dbsession):
    source = MQSource(dbsession=dbsession, model=model)
    job_id = source.call('dummy', 'echo', {'message': 'hello world'})
    dbsession.commit()

    with pytest.raises(RuntimeError) as ex:
        source.retry_job(job_id)

    assert 'job is not in a retryable state' in str(ex)


def test_retry_failed_job(model, dbsession):
    source = MQSource(dbsession=dbsession, model=model)
    job_id = source.call('dummy', 'echo', {'message': 'hello world'})
    job = source.find_job(job_id)
    job.state = model.JobStates.FAILED
    job.start_time = datetime.utcnow()
    job.end_time = datetime.utcnow()
    dbsession.commit()

    job2_id = source.retry_job(job_id)
    assert job2_id != job_id

    job = source.find_job(job2_id)
    assert job.state == model.JobStates.PENDING
    assert job.queue == 'dummy'
    assert job.method == 'echo'
    assert job.args == {'message': 'hello world'}
