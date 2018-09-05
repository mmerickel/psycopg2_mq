import sqlalchemy as sa
from sqlalchemy.dialects import postgresql as pg
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.schema import Column, CheckConstraint
from sqlalchemy.types import (
    BigInteger,
    DateTime,
    Enum,
    Integer,
    Text,
)


class Model:
    def __init__(self, Job, JobStates):
        self.Job = Job
        self.JobStates = JobStates


class JobStates:
    # constants used for the state parameter
    PENDING = 'pending'
    RUNNING = 'running'
    COMPLETED = 'completed'
    FAILED = 'failed'
    LOST = 'lost'


def make_default_model(metadata, JobStates=JobStates):
    Base = declarative_base(metadata=metadata)

    state_enum = Enum(
        JobStates.PENDING,
        JobStates.RUNNING,
        JobStates.COMPLETED,
        JobStates.FAILED,
        JobStates.LOST,
        metadata=metadata,
        name='mq_job_state'
    )

    class Job(Base):
        __table_name__ = 'mq_job'

        id = Column(BigInteger)
        start_time = Column(DateTime)
        end_time = Column(DateTime)
        state = Column(state_enum, nullable=False, index=True)

        created_time = Column(DateTime, nullable=False, index=True)
        scheduled_time = Column(DateTime, nullable=False, index=True)

        queue = Column(Text, nullable=False, index=True)
        method = Column(Text, nullable=False)
        args = Column(pg.JSONB, nullable=False)
        result = Column(pg.JSONB)

        lock_id = Column(Integer, nullable=True, unique=True)

        __table_args__ = (
            CheckConstraint(
                sa.or_(
                    state != JobStates.RUNNING,
                    lock_id != sa.null(),
                ),
                name='ck_mq_job_lock_id',
            ),
        )

        def __repr__(self):
            return (
                '<Job('
                'id={0.id}, '
                'state="{0.state}", '
                'scheduled_time={0.scheduled_time}, '
                'queue="{0.queue}", '
                'method="{0.method}"'
                ')'
            ).format(self)

    return Model(Job, JobStates)
