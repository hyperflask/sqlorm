from sqlorm import Model, SQL, Relationship, is_dirty, PrimaryKey, Column
from sqlorm.mapper import Mapper
from models import *
import abc


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


def test_inheritance():
    class A(Model, abc.ABC):
        col1: str
        col2 = Column(type=int)
        col3 = Column(type=bool)

    assert not hasattr(A, "__mapper__")
    assert isinstance(A.col1, Column)
    assert A.col3.type.sql_type == "boolean"

    class B(A):
        col3: str
        col4 = Column(type=int)

    assert B.__mapper__
    assert B.__mapper__.columns.names == ["col1", "col2", "col3", "col4", "id"]
    assert B.col3.type.sql_type == "text"


def test_mixins():
    class Mixin:
        col1: str
        col2 = Column(type=int)

    class A(Model, Mixin):
        col3 = Column(type=bool)

    assert A.__mapper__
    assert A.__mapper__.columns.names == ["col1", "col2", "col3", "id"]


def test_find_all(engine):
    listener_called = False

    @Model.before_query.connect
    def on_before_query(sender, stmt, params):
        nonlocal listener_called
        listener_called = True

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
        assert listener_called

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
    listener_called = 0

    @Model.before_refresh.connect
    def on_before_refresh(sender, obj):
        nonlocal listener_called
        listener_called += 1

    @Model.after_refresh.connect
    def on_after_refresh(sender, obj):
        nonlocal listener_called
        listener_called += 1

    with engine:
        task = Task.get(1)
        assert task.done
        task.done = False
        assert not task.done
        task.refresh()
        assert task.done
        assert listener_called == 2


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
        assert str(Task.find_todos.sql(Task)) == "SELECT tasks.id , tasks.title , tasks.completed , tasks.user_id FROM tasks WHERE not tasks.completed"
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
            "SELECT WHERE col1 = 'foo'"

        @classmethod
        def test_insert(cls):
            "INSERT INTO (col1) VALUES ('bar')"

        @classmethod
        def test_update(cls):
            "UPDATE SET col1 = 'bar'"

        @classmethod
        def test_delete(cls):
            "DELETE WHERE col1 = 'foo'"

    assert str(TestModel.find_all.sql(TestModel)) == "SELECT test.id , test.col1 FROM test WHERE col1 = 'foo'"
    assert str(TestModel.test_insert.sql(TestModel)) == "INSERT INTO test (col1) VALUES ('bar')"
    assert str(TestModel.test_update.sql(TestModel)) == "UPDATE test SET col1 = 'bar'"
    assert str(TestModel.test_delete.sql(TestModel)) == "DELETE FROM test WHERE col1 = 'foo'"


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
    listener_called = 0

    @Model.before_insert.connect
    def on_before_insert(sender, obj):
        nonlocal listener_called
        listener_called += 1

    @Model.after_insert.connect
    def on_after_insert(sender, obj):
        nonlocal listener_called
        listener_called += 1

    with engine:
        user = User(email="bar@example.com")
        assert user.__dirty__ == {"email"}
        assert not user.id
        assert user.save()
        assert user.id == 2
        assert listener_called == 2

        user = User.get(2)
        assert user.email == "bar@example.com"
        assert User.__mapper__.dehydrate(user) == dict()
        user.save() # noop

        user = User()
        assert User.__mapper__.dehydrate(user) == dict()
        user.save()
        assert user.id
        assert not user.email

    def on_before_insert_cancel(sender, obj):
        return False
    
    with Model.before_insert.connected_to(on_before_insert_cancel), engine:
        user = User(email="something@something")
        assert not user.save()
        assert not user.id

    def on_before_insert_stmt(sender, obj):
        return SQL.insert("users", {"email": "something@different"}).returning("*")
    
    with Model.before_insert.connected_to(on_before_insert_stmt), engine:
        user = User(email="something@something")
        assert user.save()
        assert user.id
        assert user.email == "something@different"


def test_update(engine):
    listener_called = 0

    @Model.before_update.connect
    def on_before_update(sender, obj):
        nonlocal listener_called
        listener_called += 1

    @Model.after_update.connect
    def on_after_update(sender, obj):
        nonlocal listener_called
        listener_called += 1

    with engine:
        user = User.get(2)
        user.email = "bar2@example.com"
        user.save()
        assert listener_called == 2

        user = User.get(2)
        assert user.email == "bar2@example.com"


def test_save(engine):
    listener_called = 0

    @Model.before_save.connect
    def on_before_save(sender, obj, is_new):
        nonlocal listener_called
        listener_called += 1

    @Model.after_save.connect
    def on_after_save(sender, obj):
        nonlocal listener_called
        listener_called += 1

    with engine:
        user = User.get(3)
        user.email = "baz@example.com"
        user.save()
        assert listener_called == 2

        user = User.get(3)
        assert user.email == "baz@example.com"

        user = User(email="boo@example.com")
        assert not user.id
        user.save()
        assert user.id == 5

        user = User.get(5)
        assert user.email == "boo@example.com"


def test_delete(engine):
    listener_called = 0

    @Model.before_delete.connect
    def on_before_delete(sender, obj):
        nonlocal listener_called
        listener_called += 1

    @Model.after_delete.connect
    def on_after_delete(sender, obj):
        nonlocal listener_called
        listener_called += 1

    with engine:
        user = User.get(4)
        user.delete()
        assert listener_called == 2

        user = User.get(4)
        assert not user
