## Drivers

sqlorm provides built-in support for the following databases:

| Database | DBAPI implementation | URI scheme | sqlorm overrides | Required package |
| --- | --- | --- | --- | --- |
| SQLite | [sqlite3](https://docs.python.org/3/library/sqlite3.html) | sqlite:// | Uses `sqlite3.Row` as the default row_factory and check_same_thread=False | |
| PostgreSQL | [psycopg3](https://www.psycopg.org/psycopg3/docs/index.html) | postgresql:// | No overrides | psycopg\[binary\] |
| MySQL | [mysql.connector](https://dev.mysql.com/doc/connector-python/en/connector-python-introduction.html) | mysql:// | Uses `MySQLCursorDict` as default cursor class | mysql-connector-python |

### Sqlite

By default, Python's sqlite3 module does not allow a connection created in one thread to be re-used in another thread.
This creates an issue with the way pooling works. Additionnaly, sqlorm's sessions being per-thread, there is a kind of guarantee that a connection object won't be used concurrently.
For this reason, the default value of `check_same_thread` is set to false by default.

Additional parameters are available when connecting:

 - *pragma*: a dict of sqlite pragmas to set on connection
 - *ext*: a list of extension modules to load on connection
 - *fine_tune*: a boolean indicating to apply fine tuned pragmas for high concurrent workloads like web servers ([explanations](https://fractaledmind.github.io/2023/09/07/enhancing-rails-sqlite-fine-tuning/))

### Postgresql

Some custom types are provided. You can also use a subclassed version of `SQL` with additional utils.

```python
from sqlorm import Model, Text
from sqlorm.drivers.postgresql import SQL, Array, JSON, JSONB

class MyModel(Model):
    array_col = Column(type=Array(Text))
    json_col = Column(type=JSON)
    jsonb_col = Column(type=JSONB)

with engine:
    rs = MyModel.find_all(MyModel.jsonb_col.op("@>", SQL.cast({"key": "value"}, JSONB))) # SELECT * FROM MyModel WHERE json_col @> ('{"key": "value"}'::jsonb)
    rs = MyModel.query(MyModel.select_from([MyModel.json_col.op("->>", "property")])) # SELECT json_col->>property FROM MyModel
```