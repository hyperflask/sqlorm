from sqlorm import Engine, Session
from sqlorm.helpers import connect_via_engine, after_commit


def test_connect_via_engine():
    e = Engine.from_uri("sqlite://:memory:")

    msg = []
    @connect_via_engine(e, Session.after_commit)
    def handler(sender, **kwargs):
        msg.append("connected")

    with e as tx:
        tx.execute("create table test (id integer primary key, name text)")

    assert msg == ["connected"]


def test_after_commit():
    e = Engine.from_uri("sqlite://:memory:")

    msg = []
    with e.session() as session:

        with session as tx:
            @after_commit
            def on_commit():
                msg.append("committed")
            tx.execute("create table test (id integer primary key, name text)")

        with session as tx:
            tx.execute("insert into test (name) values ('test')")
            
    assert msg == ["committed"]