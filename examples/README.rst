::

  docker run --rm -it -e POSTGRES_PASSWORD=seekrit -p 5432:5432 postgres:16

::

  export DBURL="postgresql+psycopg2://postgres:seekrit@localhost:5432/postgres"

::

  python examples/setup.py --url $DBURL

::

  python examples/worker.py --url $DBURL

::

  python examples/call.py --url $DBURL
