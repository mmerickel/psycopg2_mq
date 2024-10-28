import sqlalchemy as sa
from sqlalchemy.dialects import postgresql as pg
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy.schema import CheckConstraint, Column, ForeignKey, Index
from sqlalchemy.types import BigInteger, Boolean, DateTime, Enum, Integer, Text


class Model:
    channel_prefix = 'mq_'

    def __init__(
        self,
        *,
        Job,
        JobStates,
        Lock,
        JobCursor,
        JobListener,
        JobSchedule,
        JobListenerLink=None,
        JobScheduleLink=None,
    ):
        self.Job = Job
        self.JobStates = JobStates
        self.JobCursor = JobCursor
        self.JobListener = JobListener
        self.JobSchedule = JobSchedule
        self.JobListenerLink = JobListenerLink
        self.JobScheduleLink = JobScheduleLink
        self.Lock = Lock


class JobStates:
    # constants used for the state parameter
    PENDING = 'pending'
    RUNNING = 'running'
    COMPLETED = 'completed'
    FAILED = 'failed'
    CANCELED = 'canceled'
    LOST = 'lost'


def make_default_model(metadata, JobStates=JobStates):
    Base = declarative_base(metadata=metadata)

    state_enum = Enum(
        JobStates.PENDING,
        JobStates.RUNNING,
        JobStates.COMPLETED,
        JobStates.FAILED,
        JobStates.CANCELED,
        JobStates.LOST,
        metadata=metadata,
        name='mq_job_state',
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

        cursor_key = Column(Text)
        cursor_snapshot = Column(pg.JSONB)
        collapsible = Column(Boolean)

        trace = Column(pg.JSONB)

        lock_id = Column(Integer)
        worker = Column(Text)

        listener_links = relationship(lambda: JobListenerLink, back_populates='job')
        listeners = relationship(
            lambda: JobListener,
            secondary=lambda: JobListenerLink.__table__,
            back_populates='jobs',
            viewonly=True,
        )

        schedule_links = relationship(lambda: JobScheduleLink, back_populates='job')
        schedules = relationship(
            lambda: JobSchedule,
            secondary=lambda: JobScheduleLink.__table__,
            back_populates='jobs',
            viewonly=True,
        )

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
                cursor_key,
                queue,
                method,
                postgresql_where=sa.and_(
                    state == JobStates.PENDING,
                    collapsible == sa.true(),
                ),
                unique=True,
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
                ', collapsible="{0.collapsible}"'
                ')>'
            ).format(self)

    class JobCursor(Base):
        __tablename__ = 'mq_job_cursor'

        key = Column(Text, primary_key=True)
        properties = Column(pg.JSONB, default=dict, nullable=False)

        updated_job_id = Column(
            ForeignKey('mq_job.id', ondelete='set null', onupdate='cascade'),
        )
        updated_job = relationship(lambda: Job)

        def __repr__(self):
            return (
                f'<JobCursor(key="{self.key}", updated_job_id={self.updated_job_id})>'
            )

    class JobListener(Base):
        __tablename__ = 'mq_job_listener'

        id = Column(BigInteger, primary_key=True)

        created_time = Column(DateTime, nullable=False)

        is_enabled = Column(Boolean, nullable=False)

        event = Column(Text, nullable=False, index=True)

        queue = Column(Text, nullable=False, index=True)
        method = Column(Text, nullable=False)
        args = Column(pg.JSONB, nullable=False)

        # the event context will be added as an arg in the job via this key
        context_arg_key = Column(Text, nullable=False)

        # a duration in the future when the job should execute
        # relative to the event time, supports anything that the postgres
        # interval type can parse
        when = Column(pg.INTERVAL)

        cursor_key = Column(Text)
        collapse_on_cursor = Column(Boolean)

        job_links = relationship(lambda: JobListenerLink, back_populates='listener')
        jobs = relationship(
            lambda: Job,
            secondary=lambda: JobListenerLink.__table__,
            back_populates='listeners',
            viewonly=True,
        )

        def __repr__(self):
            return (
                '<JobListener('
                'id={0.id}'
                ', event="{0.event}"'
                ', queue="{0.queue}"'
                ', method="{0.method}"'
                ', when="{0.when}"'
                ', cursor_key="{0.cursor_key}"'
                ', collapse_on_cursor={0.collapse_on_cursor}'
                ', is_enabled={0.is_enabled}'
                '>'.format(self)
            )

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
        # defaults to true if cursor_key is set
        collapse_on_cursor = Column(Boolean)

        next_execution_time = Column(DateTime, nullable=True)

        jobs = relationship(lambda: Job, back_populates='schedule')

        job_links = relationship(lambda: JobScheduleLink, back_populates='schedule')
        jobs = relationship(
            lambda: Job,
            secondary=lambda: JobScheduleLink.__table__,
            back_populates='schedules',
            viewonly=True,
        )

        def __repr__(self):
            return (
                '<JobSchedule('
                'id={0.id}'
                ', queue="{0.queue}"'
                ', method="{0.method}"'
                ', rrule="{0.rrule}"'
                ', cursor_key="{0.cursor_key}"'
                ', collapse_on_cursor={0.collapse_on_cursor}'
                ', is_enabled={0.is_enabled}'
                '>'.format(self)
            )

    class JobListenerLink(Base):
        __tablename__ = 'mq_job_listener_link'

        job_id = Column(
            ForeignKey('mq_job.id', ondelete='cascade', onupdate='cascade'),
            primary_key=True,
            nullable=False,
            index=True,
        )
        job = relationship(lambda: Job, back_populates='listener_links')

        listener_id = Column(
            ForeignKey('mq_job_listener.id', ondelete='cascade', onupdate='cascade'),
            primary_key=True,
            nullable=False,
            index=True,
        )
        listener = relationship(lambda: JobListener, back_populates='job_links')

        def __repr__(self):
            return (
                '<JobListenerLink('
                'job_id={0.job_id}'
                ', listener_id={0.listener_id}'
                '>'.format(self)
            )

    class JobScheduleLink(Base):
        __tablename__ = 'mq_job_schedule_link'

        job_id = Column(
            ForeignKey('mq_job.id', ondelete='cascade', onupdate='cascade'),
            primary_key=True,
            nullable=False,
            index=True,
        )
        job = relationship(lambda: Job, back_populates='schedule_links')

        schedule_id = Column(
            ForeignKey('mq_job_schedule.id', ondelete='cascade', onupdate='cascade'),
            primary_key=True,
            nullable=False,
            index=True,
        )
        schedule = relationship(lambda: JobSchedule, back_populates='job_links')

        def __repr__(self):
            return (
                '<JobScheduleLink('
                'job_id={0.job_id}'
                ', schedule_id={0.schedule_id}'
                '>'.format(self)
            )

    class Lock(Base):
        __tablename__ = 'mq_lock'

        queue = Column(Text, primary_key=True)
        key = Column(Text, primary_key=True)

        lock_id = Column(Integer, nullable=False)
        worker = Column(Text, nullable=False)

        def __repr__(self):
            return '<Lock(queue="{0.queue}", key="{0.key}">'.format(self)

    return Model(
        Job=Job,
        JobStates=JobStates,
        JobCursor=JobCursor,
        JobListener=JobListener,
        JobSchedule=JobSchedule,
        JobListenerLink=JobListenerLink,
        JobScheduleLink=JobScheduleLink,
        Lock=Lock,
    )
