import sqlalchemy as sa
from sqlalchemy.dialects import postgresql as pg
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.schema import Column, CheckConstraint, ForeignKey, Index
from sqlalchemy.types import (
    BigInteger,
    Boolean,
    DateTime,
    Enum,
    Integer,
    Text,
)


class Model:
    channel_prefix = 'mq_'

    def __init__(self, Job, JobStates, JobCursor, JobSchedule, Lock):
        self.Job = Job
        self.JobStates = JobStates
        self.JobCursor = JobCursor
        self.JobSchedule = JobSchedule
        self.Lock = Lock


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
        __tablename__ = 'mq_job'

        id = Column(BigInteger, primary_key=True)
        start_time = Column(DateTime)
        end_time = Column(DateTime)
        state = Column(state_enum, nullable=False, index=True)

        created_time = Column(DateTime, nullable=False)
        scheduled_time = Column(DateTime, nullable=False, index=True)

        queue = Column(Text, nullable=False, index=True)
        method = Column(Text, nullable=False)
        args = Column(pg.JSONB, nullable=False)
        result = Column(pg.JSONB)

        schedule_id = Column(
            ForeignKey('mq_job_schedule.id', ondelete='cascade', onupdate='cascade'),
            index=True,
        )
        schedule = relationship('JobSchedule', backref='jobs')

        cursor_key = Column(Text)
        cursor_snapshot = Column(pg.JSONB)
        collapsible = Column(Boolean)

        lock_id = Column(Integer)
        worker = Column(Text)

        __table_args__ = (
            CheckConstraint(
                sa.or_(
                    state != JobStates.RUNNING,
                    lock_id != sa.null(),
                ),
                name='ck_mq_job_lock_id',
            ),
            Index(
                'uq_mq_job_pending_cursor_key_queue_method',
                cursor_key, queue, method,
                postgresql_where=sa.and_(
                    state == JobStates.PENDING,
                    collapsible == sa.true(),
                ),
            ),
            Index(
                'uq_mq_job_running_cursor_key',
                cursor_key,
                postgresql_where=state == JobStates.RUNNING,
                unique=True,
            ),
        )

        def __repr__(self):
            return (
                '<Job('
                'id={0.id}'
                ', state="{0.state}", '
                ', scheduled_time={0.scheduled_time}'
                ', queue="{0.queue}"'
                ', method="{0.method}"'
                ', cursor_key="{0.cursor_key}"'
                ')>'
            ).format(self)

    class JobCursor(Base):
        __tablename__ = 'mq_job_cursor'

        key = Column(Text, primary_key=True)
        properties = Column(pg.JSONB, default=dict, nullable=False)

        def __repr__(self):
            return '<JobCursor(key="{0.key}")>'.format(self)

    class JobSchedule(Base):
        __tablename__ = 'mq_job_schedule'

        id = Column(BigInteger, primary_key=True)

        created_time = Column(DateTime, nullable=False)

        is_enabled = Column(Boolean, nullable=False)

        rrule = Column(Text, nullable=False)

        queue = Column(Text, nullable=False, index=True)
        method = Column(Text, nullable=False)
        args = Column(pg.JSONB, nullable=False)

        cursor_key = Column(Text)

        next_execution_time = Column(DateTime, nullable=True)

        def __repr__(self):
            return (
                '<JobSchedule('
                'queue="{0.queue}"'
                ', method="{0.method}"'
                ', rrule="{0.rrule}"'
                ', cursor_key="{0.cursor_key}"'
                ', is_enabled={0.is_enabled}'
                '>'
                .format(self)
            )

    class Lock(Base):
        __tablename__ = 'mq_lock'

        queue = Column(Text, primary_key=True)
        key = Column(Text, primary_key=True)

        lock_id = Column(Integer, nullable=False)
        worker = Column(Text, nullable=False)

        def __repr__(self):
            return '<Lock(queue="{0.queue}", key="{0.key}">'.format(self)

    return Model(Job, JobStates, JobCursor, JobSchedule, Lock)
