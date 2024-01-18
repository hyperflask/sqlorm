from sqlorm import Engine


def test_mysql_engine():
    e = Engine.from_uri("mysql://?user=sqlorm&password=sqlorm&database=sqlorm")