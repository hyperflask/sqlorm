from sqlorm import Engine
from sqlorm.schema import create_table, create_all, create_migrations_from_dir, migrate, get_schema_version, init_db
from models import *
import os
import pytest


migrations_path = os.path.join(os.path.dirname(__file__), "migrations")


def test_create_table():
    engine = Engine.from_uri("sqlite://:memory:")
    with engine:
        assert str(create_table.sql(Task)) == "CREATE TABLE tasks ( id integer PRIMARY KEY AUTOINCREMENT , title text , completed boolean , user_id integer REFERENCES users(id) )"
        assert str(create_table.sql(User)) == "CREATE TABLE users ( id integer PRIMARY KEY AUTOINCREMENT , email text )"


def test_create_all():
    engine = Engine.from_uri("sqlite://:memory:")
    with engine:
        with pytest.raises(Exception):
            User.create()
        with pytest.raises(Exception):
            Task.create()
        create_all(logger=True)
        assert User.create()
        assert Task.create()


def test_create_migrations_from_dir():
    migrations = create_migrations_from_dir(migrations_path)
    assert len(migrations) == 3
    assert migrations[0] == (0, "init", os.path.join(migrations_path, "000_init.sql"))
    assert migrations[1] == (1, "first_insert", os.path.join(migrations_path, "001_first_insert.sql"))
    assert migrations[2] == (2, "second_insert", os.path.join(migrations_path, "002_second_insert.py"))


def test_migrate():
    engine = Engine.from_uri("sqlite://:memory:")
    with engine as tx:
        with pytest.raises(Exception):
            tx.execute("SELECT * FROM first_table")
        assert get_schema_version() is None
        migrate(migrations_path)
        assert dict(tx.fetchone("SELECT * FROM first_table")) == {"id": 1}
        assert dict(tx.fetchone("SELECT * FROM second_table")) == {"id": 1, "email": "foo@foo.com"}
        assert get_schema_version() == 2


def test_migrate_partial():
    engine = Engine.from_uri("sqlite://:memory:")
    with engine as tx:
        migrate(migrations_path, to_version=0, logger=True)
        assert get_schema_version() == 0
        assert tx.fetchone("SELECT * FROM first_table") is None
        migrate(migrations_path, logger=True)
        assert get_schema_version() == 2
        assert dict(tx.fetchone("SELECT * FROM first_table")) == {"id": 1}


def test_migrate_error():
    engine = Engine.from_uri("sqlite://:memory:")
    with engine as tx:
        with pytest.raises(Exception):
            migrate(os.path.join(os.path.dirname(__file__), "migrations-with-error"), logger=True)


def test_init_db():
    engine = Engine.from_uri("sqlite://:memory:")
    with engine:
        init_db(migrations_path, logger=True)
        assert get_schema_version() == 2

    engine = Engine.from_uri("sqlite://:memory:")
    with engine:
        init_db()
        assert get_schema_version() is None
        assert User.create()


def test_migrate_cli():
    from sqlorm.migrate import main
    main(["--path", migrations_path, "sqlite://:memory:"])