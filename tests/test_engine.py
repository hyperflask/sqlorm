from sqlorm import Engine, EngineError, Session, Transaction, EngineDispatcher, ensure_session
from sqlorm.engine import parse_uri, MissingSessionError
import sqlite3
import pytest


def test_create_engine():
    e = Engine(sqlite3, lambda dbapi: dbapi.connect(":memory:"), pool=False)
    assert e.dbapi is sqlite3
    assert e.connection_factory
    assert isinstance(e.connect(), sqlite3.Connection)

    e = Engine.from_dbapi(sqlite3, ":memory:", pool=False)
    assert e.dbapi is sqlite3
    assert e.connection_factory
    assert e.connection_factory.args == (":memory:",)
    assert e.connection_factory.kwargs == {}
    assert isinstance(e.connect(), sqlite3.Connection)

    e = Engine.from_dbapi("sqlite3", ":memory:", pool=False)
    assert e.dbapi is sqlite3

    uri = "sqlite3://:memory:?isolation_level=IMMEDIATE"
    e = Engine.from_uri(uri, pool=False)
    assert e.dbapi is sqlite3
    assert e.connection_factory.uri == uri
    assert e.connection_factory.args == (":memory:",)
    assert e.connection_factory.kwargs == {"isolation_level": "IMMEDIATE"}
    assert isinstance(e.connect(), sqlite3.Connection)


def test_parse_uri():
    uri = "sqlite://:memory:?pragma[journal_mode]=WAL&pragma[mmap_size]=2&fine_tune=True&ext=a.so&ext=b.so"
    assert parse_uri(uri) == ("sqlite", [":memory:"], {"pragma": {"journal_mode": "WAL", "mmap_size": 2}, "fine_tune": True, "ext": ["a.so", "b.so"]})


def test_pool():
    e = Engine.from_uri("sqlite://:memory:", max_pool_conns=2)
    assert e.pool is not False
    assert not e.pool
    conn = e.connect()
    assert conn
    assert e.active_conns
    assert conn in e.active_conns
    e.disconnect(conn)
    assert conn in e.pool
    assert conn is e.connect()
    assert not e.pool
    assert conn in e.active_conns
    e.disconnect_all()
    assert not e.active_conns
    assert not e.pool

    conn = e.connect()
    conn2 = e.connect()
    assert len(e.active_conns) == 2
    e.disconnect(conn)
    assert len(e.active_conns) == 1
    assert len(e.pool) == 1
    assert conn in e.pool

    with e as tx:
        tx.session.connect()
        assert not e.pool
        assert len(e.active_conns) == 2
        assert tx.session.conn is conn

    assert conn in e.pool
    e.connect()
    with pytest.raises(EngineError):
        e.connect()


def test_no_pool():
    e = Engine.from_uri("sqlite://:memory:", pool=False)
    assert e.pool is False
    conn = e.connect()
    assert not e.active_conns
    e.disconnect(conn)


def test_transaction():
    e = Engine.from_uri("sqlite://:memory:")
    with e as tx:
        assert isinstance(tx, Transaction)
        assert tx.session
        assert tx.session.engine is e
        assert not tx.virtual

        with e as tx2:
            assert tx2 is not tx
            assert tx2.virtual


def test_session():
    e = Engine.from_uri("sqlite://:memory:")
    with e.session() as sess:
        assert isinstance(sess, Session)
        assert not sess.conn
        assert sess.engine is e
        assert not sess.virtual_tx

        with sess as tx:
            assert isinstance(tx, Transaction)
            assert tx.session is sess
            assert not tx.virtual

        sess.connect()
        assert sess.conn

    with ensure_session(e) as sess:
        assert sess.virtual_tx is True

    with pytest.raises(MissingSessionError):
        with ensure_session() as sess:
            pass


def test_executemany():
    e = Engine.from_uri("sqlite://:memory:")
    with e as tx:
        tx.execute("CREATE TABLE numbers (num INTEGER)")
        tx.executemany("INSERT INTO numbers (num) VALUES (?)", [(i,) for i in range(10)])
        assert tx.fetchscalar("SELECT COUNT(*) FROM numbers") == 10


def test_fetchcomposite(engine):
    with engine as tx:
        sql = "SELECT u.email, t.title AS tasks__title FROM users u JOIN tasks t ON u.id = t.user_id"
        results = tx.fetchcomposite(sql)
        assert next(results) == {"email": "foo@example.com", "tasks": [{"title": "foo"}]}


def test_fetchhydrated(engine):
    class Task:
        pass

    with engine as tx:
        objs = tx.fetchhydrated({"task": Task}, "SELECT id AS task__id, title AS task__title FROM tasks")
        obj = next(objs)
        assert isinstance(obj, dict)
        assert "task" in obj
        assert isinstance(obj["task"], list)
        assert isinstance(obj["task"][0], Task)
        assert obj["task"][0].title == "foo"

        objs = tx.fetchhydrated([Task], "SELECT id AS Task__id, title AS Task__title FROM tasks")
        obj = next(objs)
        assert isinstance(obj, dict)
        assert "Task" in obj
        assert isinstance(obj["Task"], list)
        assert isinstance(obj["Task"][0], Task)
        assert obj["Task"][0].title == "foo"


def test_engine_dispatcher():
    e1 = Engine.from_uri("sqlite://:memory:")
    e2 = Engine.from_uri("sqlite://:memory:")
    disp = EngineDispatcher()
    # register e1 as the default and with a custom tag, e2 as replica
    disp.register(e1, tags=["master"], default=True)
    disp.register(e2, tags=["replica"])

    assert sorted(disp.tags) == sorted(["master", "replica", "default"])
    assert disp.engines == [e1, e2]

    # select_all with no tag should return the default engine
    assert disp.select_all() == [e1]

    # select_all by explicit tag
    assert disp.select_all("replica") == [e2]

    # selecting a single engine by tag should return that engine
    assert disp.select("replica") is e2

    # session(...) should produce a Transaction context manager
    with disp.session("replica") as sess:
        assert isinstance(sess, Session)
        assert sess.engine is e2

    # attribute access resolves to a selected engine for that tag
    assert disp.replica is e2

    # unknown tag should fallback to the default engine
    assert disp.select_all("unknown") == [e1]

    # using the dispatcher as a context manager should use the default engine
    with disp as tx:
        assert isinstance(tx, Transaction)
        assert tx.session.engine is e1

    