from sqlorm import Engine, EngineError, Session, Transaction
from sqlorm.engine import parse_uri
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