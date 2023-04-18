import pytest
from pytest_postgresql import factories
from pytest_postgresql.janitor import DatabaseJanitor
import sqlalchemy as sa
import sqlalchemy.orm

from psycopg2_mq import make_default_model

metadata = sa.MetaData()
_model = make_default_model(metadata)


def load_model(host, port, user, password, dbname):
    """
    Initialize the database schema with the default library model.
    """
    engine = sa.create_engine(f'postgresql://{user}:{password}@{host}:{port}/{dbname}')
    with engine.begin() as conn:
        metadata.create_all(bind=conn)


test_db_proc = factories.postgresql_proc(
    load=[load_model],
)


@pytest.fixture(scope='session')
def model():
    """
    The initialized database model objects.
    """
    return _model


@pytest.fixture
def dbengine(test_db_proc):
    """
    Create a new database per-test and return the SQLAlchemy engine.
    """
    user = test_db_proc.user
    password = test_db_proc.password
    host = test_db_proc.host
    port = test_db_proc.port
    dbname = test_db_proc.dbname

    # the janitor creates a new database for this specific test fixture,
    # copying data from the template database initialized in load_model above
    with DatabaseJanitor(
        version=test_db_proc.version,
        user=user,
        password=password,
        host=host,
        port=port,
        dbname=dbname,
    ):
        engine = sa.create_engine(
            f'postgresql://{user}:{password}@{host}:{port}/{dbname}'
        )
        try:
            yield engine
        finally:
            engine.dispose()


@pytest.fixture
def dbsession(dbengine):
    with sa.orm.Session(dbengine) as db:
        yield db
