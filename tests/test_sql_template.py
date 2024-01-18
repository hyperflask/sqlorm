from sqlorm.sql_template import SQLTemplate, SQLTemplateError
import pytest


def test_sql_template():
    stmt, params = SQLTemplate("SELECT * FROM {table} WHERE col1 = %(value)s", {"table": "foo", "value": "bar"}).render()
    assert stmt == "SELECT * FROM foo WHERE col1 = ?"
    assert params == ["bar"]


def test_nested_brackets():
    assert str(SQLTemplate("{{'a': 'a'}['a']}")) == "a"


def test_syntax_error():
    with pytest.raises(SQLTemplateError):
        SQLTemplate("SELECT * FROM {table")


def test_render_locals():
    tpl = SQLTemplate("1 + {value}")
    assert tpl.render({"value": 1})[0] == "1 + 1"