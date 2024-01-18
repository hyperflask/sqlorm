from sqlorm import Engine, EngineError, Session, Transaction
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

    e = Engine.from_uri("sqlite3://:memory:?isolation_level=IMMEDIATE", pool=False)
    assert e.dbapi is sqlite3
    assert e.connection_factory.args == (":memory:",)
    assert e.connection_factory.kwargs == {"isolation_level": "IMMEDIATE"}
    assert isinstance(e.connect(), sqlite3.Connection)


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
        assert not e.pool
        assert len(e.active_conns) == 2
        assert tx.conn is conn

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
        assert isinstance(tx.conn, sqlite3.Connection)
        assert tx.engine is e
        assert not tx.virtual

        with e as tx2:
            assert tx2 is not tx
            assert tx2.virtual


def test_session():
    e = Engine.from_uri("sqlite://:memory:")
    with e.session() as sess:
        assert isinstance(sess, Session)
        assert sess.conn
        assert sess.engine is e
        assert not sess.virtual_tx

        with sess as tx:
            assert isinstance(tx, Transaction)
            assert tx.engine is e
            assert not tx.virtual

        assert sess.conn