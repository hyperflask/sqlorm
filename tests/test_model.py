from sqlorm import Model, SQL, Relationship, is_dirty, PrimaryKey
from sqlorm.mapper import Mapper
from models import *


def test_model_registry():
    assert "Task" in Model.__model_registry__
    assert Model.__model_registry__["Task"] == Task
    assert "User" in Model.__model_registry__
    assert Model.__model_registry__["User"] == User
    assert Model.__model_registry__.getbytable("tasks") == Task


def test_model_binding(engine):
    assert Model.__engine__ is None
    BoundModel = Model.bind(engine)
    assert Model.__engine__ is None
    assert BoundModel.__engine__ == engine
    assert BoundModel.__model_registry__ != Model.__model_registry__


def test_model_repr():
    assert repr(User()) == "<User(?)>"
    assert repr(User(id=1)) == "<User(1)>"


def test_mapper():
    assert isinstance(Task.__mapper__, Mapper)

    assert isinstance(Task.table, SQL.Id)
    assert Task.table.name == "tasks"

    assert isinstance(Task.c, SQL.Cols)
    assert Task.c == Task.__mapper__.columns
    assert len(Task.c) == 4
    assert Task.c.names == ["id", "title", "completed", "user_id"]

    assert Task.c[0].name == "id"
    assert Task.c[0].attribute == "id"
    assert Task.c[0].primary_key
    assert Task.c[0].type
    assert Task.c[0].type.sql_type == "integer"
    assert Task.c.id == Task.c[0]
    assert Task.c["id"] == Task.c[0]
    assert Task.id == Task.c.id

    assert Task.c[1].name == "title"
    assert Task.c[1].attribute == "title"
    assert not Task.c[1].primary_key

    assert Task.c[2].name == "completed"
    assert Task.c[2].attribute == "done"
    assert Task.c[2].type
    assert Task.c[2].type.sql_type == "boolean"
    assert Task.done == Task.c.completed

    assert Task.c[3].name == "user_id"
    assert Task.c[3].attribute == "user_id"

    assert Task.__mapper__.primary_key == Task.c.id

    assert isinstance(User.tasks, Relationship)
    assert User.tasks.target == Task
    assert User.tasks.target_col == "user_id"
    assert User.tasks.source_col == "id"
    assert User.tasks.join_type == "LEFT JOIN"
    assert str(User.tasks.join_clause()) == "LEFT JOIN tasks ON tasks.user_id = users.id"
    assert User.tasks in User.__mapper__.relationships


def test_find_all(engine):
    with engine:
        tasks = Task.find_all().all()
        assert len(tasks) == 2
        assert isinstance(tasks[0], Task)
        assert tasks[0].id == 1
        assert tasks[0].title == "foo"
        assert tasks[0].done
        assert tasks[1].id == 2
        assert tasks[1].title == "bar"
        assert not tasks[1].done

        tasks = Task.find_all(Task.done == True).all()
        assert len(tasks) == 1

        tasks = Task.find_all("completed=true").all()
        assert len(tasks) == 1

        tasks = Task.find_all(title="unknown").all()
        assert len(tasks) == 0


def test_find_one(engine):
    with engine:
        task = Task.find_one(id=1)
        assert isinstance(task, Task)
        assert task.id == 1
        assert task.title == "foo"
        assert task.done


def test_get(engine):
    with engine:
        task = Task.get(1)
        assert isinstance(task, Task)
        assert task.id == 1
        assert task.title == "foo"
        assert task.__hydrated_attrs__ == {"id", "title", "done", "user_id"}

        assert Task.get(10) is None


def test_refresh(engine):
    with engine:
        task = Task.get(1)
        assert task.done
        task.done = False
        assert not task.done
        task.refresh()
        assert task.done


def test_relationships(engine):
    with engine:
        user = User.get(1)
        assert not User.tasks.is_loaded(user)
        assert len(user.tasks) == 1
        assert User.tasks.is_loaded(user)
        assert isinstance(user.tasks[0], Task)
        assert user.tasks[0].id == 1

        user = User.get(1, with_rels=True)
        assert User.tasks.is_loaded(user)
        assert len(user.tasks) == 1


def test_sql_methods(engine):
    assert Task.toggle.query_decorator == "update"
    assert Task.find_todos.query_decorator == "fetchall"
    assert Task.find_todos_static.query_decorator == "fetchall"

    with engine:
        assert str(Task.find_todos.sql(Task)) == "SELECT tasks.id , tasks.title , tasks.completed , tasks.user_id FROM tasks WHERE not completed"
        assert str(Task.find_todos_static.sql()) == "SELECT * FROM tasks WHERE not completed"

        task = Task.get(2)
        assert not task.done
        task.toggle()
        assert task.done

        task = Task.get(2)
        assert task.done

    class TestModel(Model):
        table = "test"

        id: PrimaryKey[int]
        col1: str

        @classmethod
        def find_all(cls):
            "WHERE col1 = 'foo'"

    assert TestModel.find_all.sql(TestModel) == "SELECT test.id , test.col1 FROM test WHERE col1 = 'foo'"


def test_dirty_tracking(engine):
    with engine:
        task = Task.get(1)
        assert not hasattr(task, "__dirty__")
        assert not is_dirty(task)
        task.title = "hello"
        assert hasattr(task, "__dirty__")
        assert is_dirty(task)
        assert task.__dirty__ == {"title"}
        assert Task.__mapper__.dehydrate(task) == {"title": "hello"}
        task.save()
        assert task.__dirty__ == set()

        assert task.done
        task.toggle()
        assert not task.done
        assert task.__dirty__ == set()
        
        task = Task()
        assert not hasattr(task, "__dirty__")
        task.title = "hello"
        assert task.title == "hello"
        assert task.__dirty__ == {"title"}
        task.save()
        assert task.title == "hello"
        assert task.__dirty__ == set()


def test_insert(engine):
    with engine:
        user = User(email="bar@example.com")
        assert user.__dirty__ == {"email"}
        assert not user.id
        user.save()
        assert user.id == 2

        user = User.get(2)
        assert user.email == "bar@example.com"
        assert User.__mapper__.dehydrate(user) == dict()
        user.save() # noop

        user = User()
        assert User.__mapper__.dehydrate(user) == dict()
        user.save()
        assert user.id
        assert not user.email


def test_update(engine):
    with engine:
        user = User.get(2)
        user.email = "bar2@example.com"
        user.save()

        user = User.get(2)
        assert user.email == "bar2@example.com"


def test_save(engine):
    with engine:
        user = User.get(3)
        user.email = "baz@example.com"
        user.save()

        user = User.get(3)
        assert user.email == "baz@example.com"

        user = User(email="boo@example.com")
        assert not user.id
        user.save()
        assert user.id == 4

        user = User.get(4)
        assert user.email == "boo@example.com"


def test_delete(engine):
    with engine:
        user = User.get(4)
        user.delete()

        user = User.get(4)
        assert not user