# Signals

Signals allow you to listen and react to events emitted by sqlorm. The [blinker](https://blinker.readthedocs.io) lib is used to implement signals.

## Engine

The following signals exist on the `Engine` class. The sender is always the engine instance.

- `connected`: receive `conn` (connection instance)
- `pool_checkout`: connection checked out of the pool, receive `conn`
- `pool_checkin`: connection returned to the pool, receive `conn`
- `disconnected`: receive `conn` (connection instance)

Example:

```py
from sqlorm import Engine

@Engine.connected.connect
def on_connected(engine, conn, from_pool):
    # do something
```

## Session

The following signals exist on the `Session` class. The sender is always the session instance.

- `before_commit`
- `after_commit`
- `before_rollback`
- `after_rollback`

Use `session.conn` to retreive the connection object (may be None if no connection have been established, use `session.connect()` in this case).

Example:

```py
from sqlorm import Session

@Session.before_commit.connect
def on_before_commit(session):
    # do something
```

## Transaction

The following signals exist on the `Transaction` class. The sender is always the transaction instance.

- `before_execute`: receive `stmt`, `params` and `many` (to distinguish between execute and executemany)
    - when called from execute: returning a cursor will stop sqlorm execution and return the cursor directly
    - when called from executemany: returning False will stop sqlorm execution
    - in both case, returning a tuple `(stmt, params)` will override stmt and params
- `after_execute`: receive `cursor`, `stmt`, `params` and `many`
- `handle_error`: receive `cursor`, `stmt`, `params`, `many` and `exc`:
    - when handling for execute: return a cursor to prevent raising the exception
    - when handling for executemany: return True to prevent raising

## Model

The following signals exist on the `Model` class. The sender is always the model class.

- `before_query`: receive `stmt` and `params` args. can override them by returning a tuple `(stmt, params)`

The following signals receive `obj` kwargs containing the model instance.
Returning `False` from any listener will cancel the operation. Returning a value will override the statement.

- `before_refresh`
- `before_insert`
- `before_update`
- `before_delete`
- `before_save` (receive a `is_new` kwargs, can only cancel operations)

The following signals are sent only if an operation is performed. They receive the `obj` kwargs containing the model instance.

- `after_refresh`
- `after_insert`
- `after_update`: receive also `updated` a boolean indicating if an update stmt was actually performed
- `after_save`
- `after_delete`

Examples:

```py
from sqlorm import Model

@Model.before_query.connect
def on_before_query(model_class, stmt, params):
    # do something

@Model.before_query.connect_via(MyModel)
def on_my_model_before_query(model_class, stmt, params):
    # do something only on before_query of MyModel

@Model.before_insert.connect
def on_before_insert(model_class, obj, stmt):
    return False # cancel the insert

@Model.before_insert.connect
def on_before_insert(model_class, obj, stmt):
    return SQL.insert(model_class.table, {"col": "value"}) # return custom statement
```