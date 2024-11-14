from contextlib import contextmanager
import pytest
import threading
import time

from psycopg2_mq import MQSource, MQWorker


def test_simple_integration(model, dbsession, worker_proxy):
    worker_proxy.start(
        queues={
            'dummy': DummyQueue(),
        }
    )

    source = MQSource(dbsession=dbsession, model=model)
    with dbsession.begin():
        job_id = source.call('dummy', 'echo', {'message': 'hello world'})

    with wait_for_job(source, job_id) as job:
        assert job.state == model.JobStates.COMPLETED
        assert job.result == {
            'queue': 'dummy',
            'method': 'echo',
            'args': {'message': 'hello world'},
        }
        assert job.start_time is not None
        assert job.end_time is not None


def test_listener_integration(model, dbsession, worker_proxy):
    worker_proxy.start(
        queues={
            'dummy': DummyQueue(),
            'listener': DummyQueue(),
        },
        threads=2,
    )

    source = MQSource(dbsession=dbsession, model=model)
    with dbsession.begin():
        listener = source.add_listener(
            'mq.job_finished.completed.dummy.echo',
            'listener',
            'listener_echo',
            {'a': 1},
        )
        listener_id = listener.id

        job_id = source.call('dummy', 'echo', {'message': 'hello world'})

    with wait_for_job(source, job_id) as job:
        assert job.state == model.JobStates.COMPLETED
        assert job.result == {
            'queue': 'dummy',
            'method': 'echo',
            'args': {'message': 'hello world'},
        }
        assert job.start_time is not None
        assert job.end_time is not None

        expected_event = {
            'name': 'mq.job_finished.completed.dummy.echo',
            'listener_id': listener_id,
            'data': {
                'id': job.id,
                'queue': job.queue,
                'method': job.method,
                'start_time': job.start_time.isoformat(),
                'end_time': job.end_time.isoformat(),
                'result': job.result,
            },
        }

        job = (
            source.query_jobs.join(model.JobListenerLink)
            .filter(model.JobListenerLink.listener_id == listener_id)
            .one()
        )
        assert job.queue == 'listener'
        assert job.method == 'listener_echo'
        assert job.args == {'a': 1, 'event': expected_event}
        job_id = job.id

    with wait_for_job(source, job_id) as job:
        assert job.state == model.JobStates.COMPLETED
        assert job.result == {
            'queue': 'listener',
            'method': 'listener_echo',
            'args': {'a': 1, 'event': expected_event},
        }
        assert job.start_time is not None
        assert job.end_time is not None


class DummyQueue:
    def execute_job(self, job):
        return {
            'queue': job.queue,
            'method': job.method,
            'args': job.args,
        }


class WorkerProxy:
    worker = None
    thread = None

    def __init__(self, model, dbengine):
        self.model = model
        self.dbengine = dbengine

    def start(self, **kw):
        self.worker = MQWorker(
            engine=self.dbengine,
            model=self.model,
            capture_signals=False,
            timeout=10,
            **kw,
        )
        self.thread = threading.Thread(target=self.worker.run)
        self.thread.daemon = True
        self.thread.start()
        return self.worker

    def stop(self):
        self.worker.shutdown_gracefully()
        self.thread.join()


@pytest.fixture
def worker_proxy(model, dbengine):
    proxy = WorkerProxy(model, dbengine)
    try:
        yield proxy
    finally:
        proxy.stop()


@contextmanager
def wait_for_job(source, job_id, *, max_wait=60):
    start = time.time()
    while True:
        if time.time() - start > max_wait:  # pragma: nocover
            raise Exception('timeout while waiting for job to finish', job_id)
        with source.dbsession.begin():
            job = source.get_job(job_id)
            if job.state in {
                source.model.JobStates.COMPLETED,
                source.model.JobStates.FAILED,
                source.model.JobStates.LOST,
            }:
                yield job
                break
            time.sleep(0.1)
