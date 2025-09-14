from sqlorm import QueryBuilder


def test_builder():
    q = QueryBuilder().select("id", "name").from_("users").where("id = 1")
    assert str(q) == "SELECT id , name FROM users WHERE id = 1"

    q = q.where("name = 'Alice'")
    assert str(q) == "SELECT id , name FROM users WHERE id = 1 AND name = 'Alice'"

    q = q.order_by("id")
    assert str(q) == "SELECT id , name FROM users WHERE id = 1 AND name = 'Alice' ORDER BY id"

    q = q.order_by("name")
    assert str(q) == "SELECT id , name FROM users WHERE id = 1 AND name = 'Alice' ORDER BY name"

    q = q.join("orders ON users.id = orders.user_id")
    assert str(q) == "SELECT id , name FROM users JOIN orders ON users.id = orders.user_id WHERE id = 1 AND name = 'Alice' ORDER BY name"

    q = q.select("id")
    assert str(q) == "SELECT id FROM users JOIN orders ON users.id = orders.user_id WHERE id = 1 AND name = 'Alice' ORDER BY name"

    q = q.join(None).where(None).where("name = 'Bob'")
    assert str(q) == "SELECT id FROM users WHERE name = 'Bob' ORDER BY name"
