import pytest
import threading
import time

from psycopg2_mq import MQSource, MQWorker


def test_integration(model, dbsession, worker_proxy):
    worker_proxy.start(
        queues={
            'dummy': DummyQueue(),
        }
    )

    source = MQSource(dbsession=dbsession, model=model)
    with dbsession.begin():
        job_id = source.call('dummy', 'echo', {'message': 'hello world'})

    while True:
        with dbsession.begin():
            job = source.find_job(job_id)
            if job.state in {
                model.JobStates.COMPLETED,
                model.JobStates.FAILED,
                model.JobStates.LOST,
            }:
                break
            time.sleep(0.1)

    job = source.find_job(job_id)
    assert job.state == model.JobStates.COMPLETED
    assert job.result == {
        'queue': 'dummy',
        'method': 'echo',
        'args': {'message': 'hello world'},
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
