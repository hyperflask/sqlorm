from sqlorm.mapper import Mapper, Column, ColumnExpr, Relationship, HydratedResultSet, PrimaryKey
from sqlorm.types import Boolean, JSON


class User:
    pass

class Task:
    pass

user_mapper = Mapper(User, "users",
                     id=Column("id", int, primary_key=True),
                     email=Column("email"))

tasks_mapper = Mapper(Task, "tasks")
tasks_mapper.map({
    "id": Column("id", primary_key=True),
    "title": Column("title"),
    "done": Column("completed", Boolean, default=False),
    "user_id": Column("user_id", lazy=True)
})

user_mapper.map("tasks", Relationship(tasks_mapper, "user_id"))


def test_mapper():
    assert user_mapper.table == "users"
    assert user_mapper.object_class == User
    assert user_mapper.columns.names == ["id", "email"]
    assert len(user_mapper.relationships) == 1
    assert user_mapper.attr_names == ["id", "email", "tasks"]
    assert user_mapper.primary_key.name == "id"
    assert user_mapper.primary_key.type
    assert user_mapper.primary_key.type.sql_type == "integer"

    assert tasks_mapper.table == "tasks"
    assert tasks_mapper.object_class == Task
    assert tasks_mapper.columns.names == ["id", "title", "completed", "user_id"]
    assert len(tasks_mapper.relationships) == 0
    assert tasks_mapper.attr_names == ["id", "title", "done", "user_id"]
    assert tasks_mapper.primary_key.name == "id"
    assert tasks_mapper.eager_columns.names == ["id", "title", "completed"]
    assert tasks_mapper.lazy_columns.names == ["user_id"]
    assert tasks_mapper.defaults == {"done": False}
    assert tasks_mapper.columns["completed"].type is Boolean
    assert tasks_mapper.columns["completed"].type.sql_type == "boolean"


def test_primary_key():
    mapper = Mapper(Task, "tasks")
    mapper.map({
        "pk": Column("pk", primary_key=True),
        "col": Column("col")
    })
    assert mapper.primary_key
    assert mapper.primary_key.name == "pk"
    assert mapper.primary_key_condition(1).render() == ("pk = ?", [1])


def test_multiple_primary_keys():
    mapper = Mapper(Task, "tasks")
    mapper.map({
        "pk1": Column("pk1", primary_key=True),
        "pk2": Column("pk2", primary_key=True),
        "col": Column("col")
    })
    assert [c.name for c in mapper.primary_key] == ["pk1", "pk2"]
    assert mapper.primary_key_condition((1, 2)).render() == ("( pk1 = ? AND pk2 = ? )", [1, 2])


def test_relationship():
    assert isinstance(user_mapper["tasks"], Relationship)
    assert user_mapper["tasks"].source_table == "users"
    assert user_mapper["tasks"].source_col == "id"
    assert user_mapper["tasks"].target_table == "tasks"
    assert user_mapper["tasks"].target_col == "user_id"
    assert user_mapper["tasks"].join_type == "LEFT JOIN"
    assert str(user_mapper["tasks"].join_clause()) == "LEFT JOIN tasks ON tasks.user_id = users.id"


def test_column_expr():
    mapper = Mapper(Task, "table")
    mapper.map({
        "col": Column("col"),
        "expr": ColumnExpr("SELECT COUNT(*) FROM subtable WHERE subtable.id = table.id", "nb_sub")
    })

    assert str(mapper.select_columns()) == "table.col , (SELECT COUNT(*) FROM subtable WHERE subtable.id = table.id) AS nb_sub"


def test_hydrate_new():
    row = {"id": 1, "email": "foo@example.com"}
    user = user_mapper.hydrate_new(row)
    assert isinstance(user, User)
    assert user.id == 1
    assert user.email == "foo@example.com"
    assert user.__hydrated_attrs__ == {"id", "email"}

    row = {"id": 1, "title": "foobar", "completed": True}
    task = tasks_mapper.hydrate_new(row)
    assert isinstance(task, Task)
    assert task.id == 1
    assert task.title == "foobar"
    assert task.done


def test_hydrate():
    row = {"id": 1, "email": "foo@example.com"}
    user = User()
    user_mapper.hydrate(user, row)
    assert user.id == 1
    assert user.email == "foo@example.com"
    assert user.__hydrated_attrs__ == {"id", "email"}


def test_dehydrate():
    user = User()
    user.id = 1
    user.email = "foo@example.com"
    user.random_attr = True

    data = user_mapper.dehydrate(user)
    assert data == {"id": 1, "email": "foo@example.com"}

    data = user_mapper.dehydrate(user, with_primary_key=False)
    assert data == {"email": "foo@example.com"}

    data = user_mapper.dehydrate(user, only=["email"])
    assert data == {"email": "foo@example.com"}

    data = user_mapper.dehydrate(user, except_=["email"])
    assert data == {"id": 1}


def test_hydrated_resultset(engine):
    with engine as tx:
        users = HydratedResultSet(tx.cursor("SELECT * FROM users"), user_mapper).all()
        assert len(users) == 1
        assert users[0].id == 1
        assert users[0].email == "foo@example.com"

        tasks = HydratedResultSet(tx.cursor("SELECT * FROM tasks"), tasks_mapper).all()
        assert len(tasks) == 2
        assert isinstance(tasks[0], Task)
        assert tasks[0].id == 1
        assert tasks[0].title == "foo"
        assert tasks[1].id == 2
        assert tasks[1].title == "bar"


def test_unknown_columns():
    row = {"id": 1, "email": "foo@example.com", "unknown": "foo"}

    user = User()
    user_mapper.hydrate(user, row, with_unknown=False)
    assert not hasattr(user, "unknown")
    assert "unknown" not in user.__hydrated_attrs__

    user = User()
    user_mapper.hydrate(user, row)
    assert hasattr(user, "unknown")
    assert user.unknown == "foo"
    assert "unknown" in user.__hydrated_attrs__

    user.unknown = "bar"
    data = user_mapper.dehydrate(user)
    assert "unknown" in data
    assert data["unknown"] == "bar"

    data = user_mapper.dehydrate(user, with_unknown=False)
    assert "unknown" not in data

    user.random_attr = "foo"
    data = user_mapper.dehydrate(user)
    assert "random_attr" not in data
    data = user_mapper.dehydrate(user, additional_attrs=["random_attr"])
    assert "random_attr" in data
    assert data["random_attr"] == "foo"

    assert user_mapper.columns.unknown.name == "unknown"


def test_disallowed_unknown_columns():
    row = {"id": 1, "email": "foo@example.com", "unknown": "foo"}
    
    user_mapper.allow_unknown_columns = False

    user = User()
    user_mapper.hydrate(user, row)
    assert not hasattr(user, "unknown")
    assert "unknown" not in user.__hydrated_attrs__

    user.unknown = "bar"
    user.__hydrated_attrs__.add("unknown")
    data = user_mapper.dehydrate(user)
    assert "unknown" not in data


def test_no_columns():
    mapper = Mapper(User, "users")

    s = mapper.select_from()
    assert str(s) == "SELECT * FROM users"


def test_select_from():
    s = user_mapper.select_from()
    assert str(s) == "SELECT users.id , users.email FROM users"

    s = tasks_mapper.select_from()
    assert str(s) == "SELECT tasks.id , tasks.title , tasks.completed FROM tasks"

    s = tasks_mapper.select_from(["completed"], "t")
    assert str(s) == "SELECT t.completed FROM tasks AS t"

    s = user_mapper.select_from(with_rels=True)
    assert str(s) == "SELECT users.id , users.email , tasks.id AS tasks__id , tasks.title AS tasks__title , tasks.completed AS tasks__completed FROM users LEFT JOIN tasks ON tasks.user_id = users.id"

    s = user_mapper.select_from(with_joins=True)
    assert str(s) == "SELECT users.id , users.email FROM users LEFT JOIN tasks ON tasks.user_id = users.id"


def test_select_by_pk():
    assert user_mapper.select_by_pk(1).render() == ("SELECT users.id , users.email FROM users WHERE id = ?", [1])


def test_lazy_columns():
    s = tasks_mapper.select_from(with_lazy=True)
    assert str(s) == "SELECT tasks.id , tasks.title , tasks.completed , tasks.user_id FROM tasks"

    s = tasks_mapper.select_from(with_lazy=["user_id"])
    assert str(s) == "SELECT tasks.id , tasks.title , tasks.completed , tasks.user_id FROM tasks"

    s = tasks_mapper.select_from(tasks_mapper.lazy_columns)
    assert str(s) == "SELECT tasks.user_id FROM tasks"
    

def test_insert():
    user = User()
    user.email = "foo@example.com"
    assert user_mapper.insert(user).render() == ("INSERT INTO users ( email ) VALUES ( ? )", ["foo@example.com"])


def test_update():
    user = User()
    user.id = 1
    user.email = "foo@example.com"
    assert user_mapper.update(user).render() == ("UPDATE users SET email = ? WHERE id = ?", ["foo@example.com", 1])


def test_delete():
    user = User()
    user.id = 1
    assert user_mapper.delete(user).render() == ("DELETE FROM users WHERE id = ?", [1])


def test_json_column():
    mapper = Mapper(Task, "table", j=Column("j", JSON))
    data = {"j": "[0, 1]"}
    class O:
        pass
    o = O()
    
    mapper.hydrate(o, data)
    assert o.j == [0, 1]

    o.j = {"key": "value"}
    assert mapper.dehydrate(o) == {"j": '{"key": "value"}'}


def test_mapper_create_from_class():
    class Task:
        id: PrimaryKey[int]
        title: str

    mapper = Mapper.from_class(Task)
    assert len(mapper.columns) == 2
    assert mapper.primary_key
    assert mapper.primary_key.name == "id"
    assert mapper.columns[0] == mapper.primary_key
    assert mapper.columns[0].type.sql_type == "integer"
    assert mapper.columns[1].name == "title"
    assert mapper.columns[1].type.sql_type == "text"