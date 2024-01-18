from sqlorm import Engine, Integer
from sqlorm.drivers.postgresql import SQL, Array, JSONB


def test_postgresql_engine():
    e = Engine.from_uri("postgresql://?user=sqlorm&password=sqlorm&dbname=sqlorm")


def test_array_type():
    assert str(Array(Integer)) == "integer[]"


def test_sql_cast():
    assert str(SQL.cast(SQL("col"), "int")) == "( col :: int )"


def test_json_op():
    assert str(SQL.Col("json_col").op("@>", SQL.cast({"key": "value"}, JSONB))) == "json_col @> ( ? :: JSONB )"