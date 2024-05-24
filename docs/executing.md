# Executing queries

Executing queries happens using transactions.

> [!IMPORTANT]
> When using sqlorm, you do not interact with cursors directly. Cursors will be created for each executed statements.

## Executing statements with no results

Use the transaction's `execute(stmt, params)` method to execute a single non-returning statement. Parameters are optional.
The first argument can be a list of statements.

```python
with engine as tx:
    tx.execute("CREATE TABLE tasks (id integer primary key, title text, done boolean)")
    tx.execute("INSERT INTO tasks (title, done) VALUES (?, ?)", ["task title", True])
```

The parameters format (placeholders and values passed as argument to `execute()`) depends on the DBAPI driver.

Use `ParametrizedStmt` to combine an sql statement and parameters:

```python
from sqlorm import ParametrizedStmt

with engine as tx:
    stmt = ParametrizedStmt("INSERT INTO tasks (title, done) VALUES (?, ?)", ["task title", True])
    tx.execute(stmt)
```

[`SQL` utilities](sql-utilities.md) to build sql statements can also be used:

```python
from sqlorm import SQL

with engine as tx:
    tx.execute(SQL.insert("tasks", {"title": "task title", "done": True}))
```

## Fetching from the database

Use the transaction's `fetch(stmt, params)` method to fetch rows from the database.
The cursor will be automatically closed when all rows have been fetched.
It returns a `ResultSet` with a few methods to handle the cursor:

- `fetch()` returns the next row in the cursor
- `first()` returns the first row and closes the cursor
- `all()` fetches all rows, closes the cursor and returns a list
- `scalar()` fetches the first row and returns the value from the first column
- `scalars()` fetches the value from the first column of each rows
- `close()` closes the cursor manually

The result set is iterable.  
There are a few shortcut methods on the transaction:

- `fetchone()` is equivalent to `fetch().first()`
- ̀`fetchall()` is equivalent to `fetch().all()`
- `fetchscalar()` is equivalent to `fetch().scalar()`
- `fetchscalars()` is equivalent to `fetch().scalars()`

```python
with engine as tx:
    task_row = tx.fetch("SELECT * FROM tasks WHERE id = ?", [1]).first()
    task_row = tx.fetchone("SELECT * FROM tasks WHERE id = ?", [1]) # same as line before

    for row in tx.fetch("SELECT * FROM tasks"):
        print(row)

    count = tx.fetchscalar("SELECT COUNT(*) FROM tasks")
    titles = tx.fetchscalars("SELECT title FROM tasks")
```

The `loader` argument on `fetch()` can be used to process each rows:

```python
with engine as tx:
    titles = tx.fetchall("SELECT title FROM tasks", loader=lambda r: r["title"])
```

## Fetching objects

Use the `model` argument of `fetch()` to fetch rows as objects.

```python
class Task:
    pass

with engine as tx:
    tasks = tx.fetch("SELECT * FROM tasks", model=Task)
    task1 = tx.fetchone("SELECT * FROM tasks WHERE id = 1", model=Task)
```

> [!NOTE]
> - The process of populating objects with data from the database is called "hydration".
> - When creating objects, `__init__()` will be bypassed as well as any custom `__setattr__`.
>
> Learn more about sqlorm hydration process in the [mapper section](mapper.md#hydrate-objects)

You can update existing objects by passing an object instance to the `obj` argument.
In this case, only the first row will be used to hydrate the object. The cursor is then closed.

```python
with engine as tx:
    task = Task()
    tx.fetch("INSERT INTO tasks (title) VALUES ('task title') RETURNING *", obj=task)
    assert task.id
    assert task.title == "task title"
```

[Models](models.md) and [mappers](mapper.md) can be used to customize how objects are mapped.
`Mapper` instances can also be provided as models.

## Composite rows

### What are composite rows ?

It is common to use joins to fetch a row with its related rows at the same time. We then need to process
the row to extract the composited rows. Because of the way joining works, a single composited rows can be represented as
multiple returned rows. sqlorm handles this with a custom cursor iterator that will combine multiple rows and allow
you to extract hierarchical data out of your results.

To do so, sqlorm relies on column aliases. Column aliases will represent the path under which these composite
rows will be nested. Use a double underscore (`__`) to separate each path segments.

Example:

```sql
SELECT
    posts.id,
    posts.title,
    posts.posted_at
    comments.id AS comments__id
    comments.author AS comments__author
    comments.message AS comments__message
FROM
    posts
    LEFT JOIN comments ON comments.post_id = posts.id
```

This query would return results like this:

| id | title | posted_at | comments__id | comment__author | comments__message |
| --- | --- | --- | --- | --- | --- |
| 1 | First post | 2024-01-01 | 1 | John | First ! |
| 1 | First post | 2024-01-01 | 2 | Paul | You make grammar mistakes... |
| 2 | Second post | 2024-01-02 | | | |

And sqlorm will process them into the following structure:

```json
[
    {
        "id": 1,
        "title": "First post",
        "posted_at": "2024-01-01",
        "comments": [
            {
                "id": 1,
                "author": "John",
                "message": "First !"
            },
            {
                "id": 2,
                "author": "Paul",
                "message": "You make grammar mistakes..."
            }
        ]
    },
    {
        "id": 2,
        "title": "Second post",
        "posted_at": "2024-01-02"
    }
]
```

### Fetching composite rows

To fetch composite rows, use `fetchcomposite()`. It has a similar API to `fetch()`.

```python
with engine as tx:
    rows = tx.fetchcomposite("SELECT ... FROM posts LEFT JOIN comments ...")
```

Using the [`SQL` utilities](sql-utilities.md) makes it easier to build these kind of queries:

```python
with engine as tx:
    stmt = SQL.select(SQL.List([
        SQL.Cols(["id", "title", "posted_at"], table="posts"),
        SQL.Cols(["id", "author", "message"], table="comments", prefix="comments__")
    ])).from_("posts").left_join("comments").on("comments.post_id = posts.id")

    rows = tx.fetchcomposite(stmt)
```

### Fetching composite objects

The `model` argument can be used to return objects. The `nested` argument can be used
to indicate what class to use for nested rows:

```python
class Comment:
    pass

with engine as tx:
    stmt = "..."
    tasks = tx.fetchcomposite(stmt, model=Task, nested={"comments": Comment})
```

`fetchhydrated()` is an alias of `fetchcomposite()` where the model argument comes first:

```python
with engine as tx:
    tasks = tx.fetchhydrated(Task, stmt)
```
