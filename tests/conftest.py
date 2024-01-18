import pytest
import logging
from sqlorm import Engine


logging.getLogger("sqlorm").setLevel(logging.DEBUG)


@pytest.fixture(scope='module')
def engine():
    db = Engine.from_uri("sqlite://:memory:", logger=True)
    with db as tx:
        tx.execute((
            "CREATE TABLE tasks (id integer primary key, title text, completed bool, user_id int)",
            "CREATE TABLE users (id integer primary key, email text)",
            "INSERT INTO users VALUES (1, 'foo@example.com')",
            "INSERT INTO tasks VALUES (1, 'foo', true, 1), (2, 'bar', false, null)"
        ))
    return db