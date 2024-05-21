# The Engine

An engine is basically a connection manager for a DBAPI compatible module.
It takes a dbapi module and a connection factory as argument.

```python
from sqlorm import Engine
import sqlite3

engine = Engine(sqlite3, lambda dbapi: dbapi.connect(":memory:"))
```

This can be simplified using `Engine.from_dbapi()` which takes the dbapi module as first argument and then any arguments for the `connect()` function.

```python
engine = Engine.from_dbapi(sqlite3, ":memory:")
```

Any DBAPI compatible database module can be used with sqlorm **as long as it returns dict-compatible object for rows**.
Implementations that return tuples (the default in the case of sqlite3 for example) are not directly compatible.
In this case, provide a custom connection factory (using `Engine.from_dbapi()`) that configure things nicely.

> [!TIP]
> The module name can be provided as a string instead of a module reference.
> When provided as a string, sqlorm will first try to import the specified module under the `sqlorm.drivers` package.
> If it fails, it will import the specified module directly.
>
> In the case of sqlite, you can thus do `Engine.from_dbapi("sqlite", ":memory:")` to use sqlorm provided sqlite3 override.

Finally, `Engine.from_uri()` provides an alternative way where an URI is used in the following way:

- the scheme specifies the dbapi module name
- the hostname+path part is passed as argument (optional)
- the query string (after ?) is parsed and passed as keyword arguments (optional)
    - *True* and *False* strings are parsed as boolean
    - numbers are parsed as int
    - keys can be repeated to create a list (eg: `foo=bar&foo=buz` is parsed to `foo=["bar", "buz"]`)
    - keys with brackets are resolved as dict (eg: `foo[bar]=value` is parsed to `foo={"bar": "value"}`)

```python
engine = Engine.from_uri("sqlite://:memory:") # sqlite instead of sqlite3 to use sqlorm's sqlite override
engine = Engine.from_uri("psycopg://?host=localhost&port=5432") # using the dbapi module name
```

Once initialized, an engine can be used as a context to start sessions.

Connections are pooled and re-used by default. You can disabled this behavior by using `pool=False` in the engine constructor.
`max_pool_conns` can also be used to define the maximum number of connections to start.
