from sqlorm import fetchall, fetchone, fetchscalar, fetchscalars, execute, update, sqlfunc
from sqlorm.sql import render
from sqlorm.sqlfunc import is_sqlfunc


def test_fetchall(engine):
    @fetchall
    def query():
        return "SELECT * FROM tasks"
    
    assert query.query_decorator == "fetchall"
    assert query.sql() == "SELECT * FROM tasks"
    assert query.is_method == False
    assert query.model is None
    assert query.engine is None

    with engine:
        r = list(query())
        assert len(r) == 2
        assert dict(r[0]) == {"id": 1, "title": "foo", "completed": True, "user_id": 1}
        assert dict(r[1]) == {"id": 2, "title": "bar", "completed": False, "user_id": None}


def test_fetchone(engine):
    @fetchone
    def query():
        return "SELECT * FROM tasks WHERE id=1"
    
    with engine:
        r = query()
        assert dict(r) == {"id": 1, "title": "foo", "completed": True, "user_id": 1}


def test_fetchscalar(engine):
    @fetchscalar
    def query():
        return "SELECT title FROM tasks WHERE id=1"
    
    with engine:
        assert query() == "foo"


def test_fetchscalars(engine):
    @fetchscalars
    def query():
        return "SELECT title FROM tasks"
    
    with engine:
        assert list(query()) == ["foo", "bar"]


def test_execute(engine):
    @execute
    def query():
        return "INSERT INTO tasks (id, title, completed) VALUES (3, 'hello world', false)"
    
    with engine as tx:
        query()
        r = tx.fetchone("SELECT * FROM tasks WHERE id=3")
        assert dict(r) == {"id": 3, "title": "hello world", "completed": False, "user_id": None}


def test_update(engine):
    @update
    def query():
        return "INSERT INTO tasks (id, title, completed) VALUES (4, 'hello', true) RETURNING *"
    
    class M:
        pass
    
    with engine as tx:
        o = M()
        query()(o)
        assert o.id == 4
        assert o.title == "hello"
        assert o.completed
        assert o.user_id is None


def test_sqlfunc():
    @sqlfunc
    def func():
        "SELECT * FROM tasks"

    assert func.query_decorator == "fetchall"
    assert str(func.sql()) == "SELECT * FROM tasks"

    @sqlfunc
    @fetchone
    def func():
        "SELECT * FROM tasks LIMIT 1"

    assert func.query_decorator == "fetchone"
    assert str(func.sql()) == "SELECT * FROM tasks LIMIT 1"

    @sqlfunc.fetchone
    def func():
        "SELECT * FROM tasks LIMIT 1"

    assert func.query_decorator == "fetchone"

    @sqlfunc
    def func():
        "INSERT INTO tasks (5, 'foobar')"

    assert func.query_decorator == "execute"
    assert str(func.sql()) == "INSERT INTO tasks (5, 'foobar')"

    @sqlfunc
    def func():
        "INSERT INTO tasks (5, 'foobar') RETURNING *"

    assert func.query_decorator == "update"
    assert str(func.sql()) == "INSERT INTO tasks (5, 'foobar') RETURNING *"

    @sqlfunc
    def func(title):
        "SELECT * FROM tasks WHERE title = %(title)s"

    assert func.query_decorator == "fetchall"
    stmt, params = render(func.sql("title"))
    assert stmt == "SELECT * FROM tasks WHERE title = ?"
    assert params == ["title"]


def test_is_sqlfunc():
    def func():
        return "hello"
    
    assert not is_sqlfunc(func)

    def func():
        "SELECT * FROM table"

    assert is_sqlfunc(func)

    def func():
        """SELECT *
        FROM table"""

    assert is_sqlfunc(func)

    @sqlfunc
    def func():
        "SELECT * FROM table"

    assert not is_sqlfunc(func)