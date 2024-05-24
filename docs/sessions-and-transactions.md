# Sessions and transactions

Sessions are active connections to the database. A session can have multiple transactions as part of its lifecycle.

When using the engine object as a context, a session and a transaction are created. The transaction will be commited
on context exit if no error are raised, rollbacked otherwise.

```python
with engine as tx:
    tx.execute("INSERT INTO ...")
    # auto commited

with engine as tx:
    tx.execute("INSERT INTO ...")
    raise Exception()
    # rollback
```

The next section covers [how to use transactions](executing.md).

> [!NOTE]
> You can create transactions inside transactions with no impact (only the top-most transaction will commit)
>
> ```python
> with engine as tx:
>     tx.execute("INSERT INTO ...")
>     with engine as tx:
>         # ...
> ```

In most cases, using the engine as the context is the most straighforward method. However, in some circumstances
you want to have an active session to be able to run select queries and eventually start transactions to perform
modifications. This is a common use case in web applications.

```python
with engine.session() as sess:
    todos = Task.find_all(done=False)

    with sess as tx:
        Task.create(title="my task")
        # commit

    todos = Task.find_all(done=False)
    # rollback
```

> [!WARNING]
> Session contexts are scoped to threads using thread locals.  
> This means that using drivers which are not thread-safe (like sqlite) is not an issue as long as you are using the engine
> to start sessions. Doing `with engine.session()` and `with engine:` in different threads will start different sessions.

`transaction()` can be used to create a transaction in the currently available session context
when the session context is not directly accessible:

```python
from sqlorm import transaction

with engine.session():
    with transaction() as tx:
        Task.create(title="my task")
        # commit
```

To ensure that a transaction is available, use `ensure_transaction()`. This will ensure
a session is available and create a virtual transaction (ie. which neither commit or rollback).

```python
from sqlorm import ensure_transaction

def count_tasks():
    with ensure_transaction() as tx:
        return tx.fetchscalar("SELECT COUNT(*) FROM tasks")

with engine:
    c = count_tasks() # works
    # commit

with engine.session():
    c = count_tasks() # works
    # rollback

count_tasks() # raises MissingEngineError
```

> [!TIP]
> `Session` can be created using raw DBAPI connection objects
>
> ```python
> from sqlorm import Session
> import sqlite3
>
> conn = sqlite3.connect(":memory:")
>
> with Session(conn) as sess:
>     with sess as tx:
>         # ...
> ```
