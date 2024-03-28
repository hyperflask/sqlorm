# sqlorm

A new kind or ORM that do not abstract away your database or SQL queries.

- Thin layer on top of [DBAPI 2.0](https://peps.python.org/pep-0249/)
- Use SQL as usual
- Utilities to help you build composable SQL queries
- ActiveRecord-style ORM that is mostly direct row to object
- Supports optional model definition via annotations
- Predictable SQL generation from models
- Supports relationships
- Built-in SQL based migration system
- Low coupling between different parts of the library which can be used directly with DBAPI Connections
- High test coverage (90%)
- Easy to understand code

Example usage:

```python
from sqlorm import Engine, Model, PrimaryKey, sqlfunc, create_table
import datetime

class Task(Model):
    table = "tasks"

    id: PrimaryKey[int]
    title: str
    done: bool
    done_at: datetime.date

    @staticmethod
    def find_todos():
        "SELECT * FROM tasks WHERE not done"

    def toggle(self):
        "UPDATE tasks SET done = not done WHERE id = %(self.id)s RETURNING *"

@sqlfunc
def tasks_completion_report(start_date, end_date):
    """SELECT done_at, COUNT(*) count
       FROM tasks
       WHERE done_at >= %(start_date)s AND done_at <= %(end_date)s
       GROUP BY done_at"""

engine = Engine.from_uri("sqlite://:memory:")
with engine:
    create_table(Task)

    task = Task(title="task1")
    task.done = False
    task.save()

    task = Task.create(title="task2", done=False)

    todos = Task.find_todos()

    task = Task.get(1)
    task.toggle()

    task = Task.get(2)
    task.title = "renamed task2"
    task.save()

    report = tasks_completion_report(datetime.date(2024, 1, 1), datetime.date.today())
```

Install using pip: `pip install sqlorm-py`

See [Flask-SQLORM](https://github.com/hyperflask/flask-sqlorm) for a Flask integration.

Table of content:

1. [Getting started](#getting-started)
2. [The Engine](#the-engine)
3. [Sessions and transactions](#sessions-and-transactions)
4. [Executing queries](#executing-queries)
5. [SQL Functions](#sql-functions)
6. [Models](#models)
7. [SQL utilities](#sql-utilities)
8. [Mapping any class](#mapping-any-class)
9. [Managing the schema](#managing-the-schema)
10. [Database specific](#database-specific)

## Getting started

Create an [`Engine`](#the-engine) that will manage database connections for you:

```python
from sqlorm import Engine

engine = Engine.from_uri("sqlite://:memory:")
```

Use the engine as a [context to connect to the database](#sessions-and-transactions). A transaction object is provided to execute statements.
If no exception is raised in the context, the transaction will be committed, rollbacked otherwise.

sqlorm's `Transaction` has a similar API than DBAPI's Connection but instead of using a cursor,
methods return [result sets](#fetching-from-the-database) that you can iterate over:

```python
with engine as tx:
    tx.execute("INSERT INTO tasks (title, done) VALUES ('task 1', false)")
    todos = tx.fetchall("SELECT * FROM tasks WHERE not done")
    task1 = tx.fetchone("SELECT * FROM tasks WHERE id = ?", [1])
    print(task1["title"])
```

Each row above is the object returned by the underlying DBAPI driver.

To fetch objects, you can pass a class using the `model` argument:

```python
class Task:
    pass

with engine as tx:
    todos = tx.fetchall("SELECT * FROM tasks WHERE not done", model=Task)
    task1 = tx.fetchone("SELECT * FROM tasks WHERE id = ?", [1], model=Task)
    print(task1.title)
```

To facilitate managing sql statements, you can create ["sql functions"](#sql-functions).
These are functions which only have a doc string containing the SQL statement.

```python
from sqlorm import sqlfunc
import datetime

@sqlfunc(model=Task) # model is optional in which case it returns the rows directly
def fetch_todos():
    "SELECT * FROM tasks WHERE not done"

@sqlfunc.fetchone(model=Task) # specify the type of fetch to do
def fetch_most_recent_task():
    "SELECT * FROM tasks ORDER BY created_at DESC LIMIT 1"

@sqlfunc.fetchscalar
def count_tasks_created_between(start_date, end_date):
    # Sql functions work similarly to f-strings, code in curly braces is evaluated and added to the sql
    # code in python format placeholders %()s is evaluated and added as parameters.
    # You can use function parameters
    "SELECT COUNT(*) FROM tasks WHERE created_at >= %(start_date)s AND created_at <= %(end_date)s"

with engine:
    todos = fetch_todos()
    task = fetch_most_recent_task()
    count = count_tasks_created_between(datetime.date.today() - datetime.timedelta(days=1), datetime.date.today())
```

Note: these functions must be executed in the context of a database session (or be bound to an engine)

Finally, we can create [model classes](#models) that can provide custom mapping information:

```python
from sqlorm import Model, PrimaryKey, create_table

class Task(Model):
    table = "tasks" # optional, use the class name by default

    # these annotations are not needed but they provide auto completion and 
    # allow creating the table using create_table()
    id: PrimaryKey[int]
    title: str
    done: bool

    @classmethod
    def find_all_todos(cls):
        "SELECT * FROM tasks WHERE not done"

    def toggle(self):
        # using RETURNING makes sure the object is updated with the new value
        "UPDATE {self.table} SET done = not done WHERE id = %(self.id)s RETURNING done"

with engine as tx:
    create_table(Task) # easily create a table from the model definition

    # call your sql method and retreive a list of Task
    todos = Task.find_all_todos()

    # perform sql queries and get Task objects
    # all the following operations are the same
    todos = tx.fetchall("SELECT * FROM tasks WHERE not done", model=Task)
    todos = Task.query("SELECT * FROM tasks WHERE not done")
    todos = Task.query(Task.select_from().where("not done"))
    todos = Task.query(Task.select_from().where(Task.done == False))
    todos = Task.find_all(Task.done == False)
    todos = Task.find_all("not done") # sql
    todos = Task.find_all(done=False)

    task = Task(title="second task", done=True)
    task.save() # executes the insert statement immediatly

    # get by primary key
    task = Task.get(1)
    task.toggle()
```

Read further for a more in depth tour of sqlorm.

## The Engine

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

By default, connections are pooled and re-used. You can disabled this behavior by using `pool=False` in the engine constructor.
`max_pool_conns` can also be used to define the maximum number of connections to start.

### Drivers

Any DBAPI compatible database module can be used with sqlorm **as long as it returns dict-compatible object for rows**.
Implementations that return tuples (the default in the case of sqlite3 for example) are not directly compatible.
In this case, provide a custom connection factory (using `Engine.from_dbapi()`) that configure things nicely.

sqlorm provides built-in support for the following databases:

| Database | DBAPI implementation | URI scheme | sqlorm overrides | Required package |
| --- | --- | --- | --- | --- |
| SQLite | [sqlite3](https://docs.python.org/3/library/sqlite3.html) | sqlite:// | Uses `sqlite3.Row` as the default row_factory and check_same_thread=False | |
| PostgreSQL | [psycopg3](https://www.psycopg.org/psycopg3/docs/index.html) | postgresql:// | No overrides | psycopg\[binary\] |
| MySQL | [mysql.connector](https://dev.mysql.com/doc/connector-python/en/connector-python-introduction.html) | mysql:// | Uses `MySQLCursorDict` as default cursor class | mysql-connector-python |

## Sessions and transactions

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

The next section covers [how to use transactions](#executing-queries).

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

> [!IMPORTANT]
> sqlorm does not assume anything regarding thread safety. However, session contexts are scoped to each thread using thread locals.  
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
> `Session` and `Transaction` objects can be created using raw DBAPI connection objects
>
> ```python
> from sqlorm import Session, Transaction
> import sqlite3
>
> conn = sqlite3.connect(":memory:")
>
> with Transaction(conn) as tx:
>     # ...
>
> with Session(conn) as sess:
>     with sess as tx:
>         # ...
> ```

## Executing queries

Executing queries happens using transactions.

> [!IMPORTANT]
> When using sqlorm, you do not interact with cursors directly. Cursors will be created for each executed statements.

### Executing statements with no results

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

[`SQL` utilities](#sql-utilities) to build sql statements can also be used:

```python
from sqlorm import SQL

with engine as tx:
    tx.execute(SQL.insert("tasks", {"title": "task title", "done": True}))
```

### Fetching from the database

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

### Fetching objects

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
> Learn more about sqlorm hydration process in the [mapper section](#hydrate-objects)

You can update existing objects by passing an object instance to the `obj` argument.
In this case, only the first row will be used to hydrate the object. The cursor is then closed.

```python
with engine as tx:
    task = Task()
    tx.fetch("INSERT INTO tasks (title) VALUES ('task title') RETURNING *", obj=task)
    assert task.id
    assert task.title == "task title"
```

[Models](#models) and [mappers](#mapping-any-class) can be used to customize how objects are mapped.
`Mapper` instances can also be provided as models.

### Composite rows

#### What are composite rows ?

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

#### Fetching composite rows

To fetch composite rows, use `fetchcomposite()`. It has a similar API to `fetch()`.

```python
with engine as tx:
    rows = tx.fetchcomposite("SELECT ... FROM posts LEFT JOIN comments ...")
```

Using the [`SQL` utilities](#sql-utilities) makes it easier to build these kind of queries:

```python
with engine as tx:
    stmt = SQL.select(SQL.List([
        SQL.Cols(["id", "title", "posted_at"], table="posts"),
        SQL.Cols(["id", "author", "message"], table="comments", prefix="comments__")
    ])).from_("posts").left_join("comments").on("comments.post_id = posts.id")

    rows = tx.fetchcomposite(stmt)
```

#### Fetching composite objects

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

## SQL functions

SQL functions are python functions with only a docstring containing an SQL statement.
To declare an SQL function, use the `sqlfunc` decorator. Unless bound to an engine
using the `engine` argument, sql functions require a session context to be used.

```python
from sqlorm import sqlfunc

@sqlfunc
def fetch_todos():
    "SELECT * FROM tasks WHERE not done"

with engine:
    todos = fetch_todos()
```

The type of fetching being done can be controlled using `@sqlorm.fetchSTYLE()` instead:

```python
from sqlorm import sqlfunc

@sqlfunc.fetchscalar
def count_todos():
    "SELECT COUNT(*) FROM tasks WHERE not done"

with engine:
    nb_todos = count_todos()
```

The same arguments as fetch can be provided to the decorator.

If you do not expect any return from your statement, use `@sqlfunc.execute`.

The content of the docstring behaves similarly to f-strings:

 - code in curly braces will be evaluated and be inserted in the final statement as is
 - code in python format markers will be evaluated and inserted as parameters

The function arguments are available in the evaluation context.
The `SQL` utility as well as `datetime` and `uuid` are also available in the evaluation context.

```python
from sqlorm import sqlfunc

@sqlfunc.fetchscalar
def count_since(table, created_after, date_column="created_at"):
    "SELECT COUNT(*) FROM {table} WHERE {date_column} >= %(created_after)s"

with engine:
    tasks2024 = count_since("tasks", "2024-01-01")
```

To get the sql statement of a function, use the `sql()` method on the function itself. It takes the
same arguments as the decorated function. It returns an `SQLTemplate` object, itself a subclass of `SQL`.

```python
print(count_since.sql("tasks", "2024-01-01"))
```

> [!TIP]
> You can also create "sql functions" without using sql statements as docstrings using query decorators.
> Your function should then return a statement to execute (possibly parameterized using `ParametrizedStmt` or `SQL`).
> 
> Available decorators are: `fetchall`, `fetchone`, `fetchscalar`, `fetchscalars`, `fetchcomposite`, `execute`
> 
> ```python
> from sqlorm import fetchall, execute
> 
> @fetchall
> def count_since(table, created_after, date_column="created_at"):
>     return SQL.select("COUNT(*)").from_(table).where(SQL(date_column) >= SQL.Param(created_after)) # same as previous example
> 
> @execute
> def insert_task(title):
>     return SQL.insert("tasks", {"title": title})
> 
> with engine:
>     tasks2024 = count_since("tasks", "2024-01-01")
>     insert_task("my todo")
> ```
>
> Under the hood, `@sqlfunc` converts the docstring-only function to a function returning an `SQLTemplate`
> and wraps it in a query decorator

## Models

Any class can be used as a model. However, subclassing sqlorm's `Model` class provides a number of advantages:

- ActiveRecord style methods to quickly fetch / save the object
- Tracking of dirty attributes to only save attributes that have been modified
- Auto fetching lazy attributes and relationships when accessed

> [!NOTE]
> When using classes that do not subclass `Model`, `Mapper.from_class()` is used to generate a mapping. See [Mapping any class](#mapping-any-class).

### Defining models

Models are classes inheriting from `Model`. To define which table they represent, use the `table` class property.

To define column mapping, define properties via annotations. The type used will be [converted to an sql type](#column-types).
For more control over the mapping, you can use instantiate `Column()` objects:

```python
from sqlorm import Model

class Task(Model):
    table = "tasks"

    id: int
    title: str = Column(type="varchar(20)") # set the column type (used in create_table())
    done = Column("completed", bool, default=False) # no annotation, column name is "completed" but property name will be "done"
```

Once columns are defined via annotations or `Column` properties, they are accessible as class and instance properties.

As class properties, they can be used as a [composable piece of sql](#sql-utilities):

```python
stmt = SQL("SELECT * FROM tasks WHERE", Task.done == False)
```

As instance properties, they can be get or set like any other property. Lazy loading and dirty flagging is performed accordingly.

```python
task = Task()
task.title = "title"
```

> [!TIP]
> To create abstract models which are not meant to be mapped, make sure your class subclasses `abc.ABC`
>
> 
> ```python
> import abc
> 
> class MyBaseModel(Model, abc.ABC):
>     # ...
>
> class MyModel(MyBaseModel):
>     # ...
> ```

### SQL methods on models

Create SQL methods as you would [SQL functions](#sql-functions). Use `@classmethod` and `@staticmethod` decorator when needed.

```python
class Task(Model):
    @classmethod
    def find_todos(cls):
        "SELECT * FROM tasks WHERE NOT done"

    def toggle(self):
        "UPDATE tasks SET done = NOT done WHERE id = %(self.id)s RETURNING *"

with engine:
    todos = Task.find_todos() # SELECT * FROM tasks WHERE NOT done

    task = Task.get(1)
    task.toggle() # UPDATE tasks SET done = NOT done WHERE id = 1 RETURNING *
```

By default, the following things happen:

- SELECT statements use `fetchhyddrated(Model, ...)`
- INSERT and UPDATE statements use `execute()` unless `RETURNING` is used in the statement,
  in which case it will update the object with the fetched value (using `fetchhydrated(Model, ..., obj=self)`)
- DELETE statements use `execute()`

You can change this default behavior using query decorators: `fetchall`, `fetchone`, `fetchscalar`, `fetchscalars`, `execute`, `update`.

```python
from sqlorm import update

class Task(Model):
    @update # update the object using the fetched row
    def refresh_title(self):
        "SELECT title FROM tasks WHERE id = %(self.id)s"

with engine:
    task = Task(id=1)
    task.refresh_title() # SELECT title FROM tasks WHERE id = 1
    assert task.title
```

To prevent repeating youself a lot when writing sql statements, you can create methods using query decorators directly:

```python
from sqlorm import Model, fetchall, update, SQL

class Task(Model):
    @classmethod
    @fetchall
    def find_todos(cls):
        return cls.select_from().where(done=False)

    @update
    def toggle(self):
        return SQL.update(self.table, {"done": SQL("not done")}).where(id=self.id).returning("*")
```

> [!NOTE]
> Using `select_from()` also ensures that the column list, lazy and eager loading are respected.

### Querying model objects

Models can be used like any other classes with the `model` argument of the fetch api.
However, `Model` also exposes easier to use ways to fetch data using without the need of explicitely passing a transaction.
When called out of a transaction context, a non commited transaction will automatically be started. If bound to an engine,
these class methods can also be called out of a session context.

- `query()` executes the provided statement using [`fetchhydrated()`](#fetching-composite-objects)
- `find_all()` constructs a select statement based on the provided arguments and executes using `query()`
- `find_one()` same as `find_all()` but only returns the first row
- `get()` to find one row by primary key

The two finder methods can take as argument a where condition (sql string) or keyword arguments representing attributes to filter by.

```python
with engine:
    todos = Task.query("SELECT * FROM tasks WHERE NOT done")
    todos = Task.find_all("NOT done")
    todos = Task.find_all(Task.done==False)
    todos = Task.find_all(done=False)
    task = Task.find_one("id=1")
    task = Task.get(1)
```

> [!TIP]
> You can also build select statement with auto populated column list and from clause using `Model.select_from()`.

Mapped columns can easily be used as [pieces of composable sql](#sql-utilities): accessing the class attribute representing
the column returns an [`SQL.Col` object](#manage-list-of-sql-pieces) that can be used with python operators to return
sql conditions:

```python
import datetime

with engine:
    todos = Task.find_all(Task.done==False & Task.created_at<datetime.date.today())
```

### Manipulating model objects

Manipulate model objects as you would with any python objects. The following methods help you execute DML statements:

- `save()` executes `insert()` or `update()` depending on the fact that the object has a primary key or not
- `insert()` executes an insert statement
- `update()` executes an update statement
- `delete()` deletes a delete statement
- `refresh()` executes a select statement (same as `get()`) and updates the object attribute values
- `create()` a class method to create and insert an object in one line

> [!NOTE]
> DML (Data Manipulation Language) statements are the statement that modify data in the database (insert, update and delete mostly)

The data used to insert or update will be limited to "dirty" attributes, which means attributes that have been
modified since the last DML statement. Setting an attribute will automatically flag it as dirty.

```python
from sqlorm import is_dirty

with engine:
    task = Task.create(title="my task") # INSERT INTO tasks (title) VALUES ('my task')

    task = Task()
    task.title = "my task"
    task.save() # INSERT INTO tasks (title) VALUES ('my task')
    # same as task.insert()

    task = Task.get(1)
    task.title = "renamed task"
    assert is_dirty(task) == True
    task.save() # UPDATE tasks SET title = 'renamed task' WHERE id = 1
    # same as task.update()

    task = Task.get(2)
    task.delete() # DELETE FROM tasks WHERE id = 2

    task = Task()
    task.id = 1
    task.refresh() # SELECT * FROM tasks WHERE id = 1
```

> [!TIP]
> You can also manually flag an attribute as dirty using `flag_dirty_attr(obj, attr)`.
> Dirty attributes are stored in the object's `__dirty__` attribute.

If you do not wish to use dirty tracking, you can disable it in the model definition:

```python
class MyBaseModel(Model, abc.ABC):
    class Meta:
        insert_update_dirty_only = False
```

> [!TIP]
> Which engine is used when manipulating models entirely depends on the
> current session context. Additionnally, sqlorm do not track objects globally.  
> This means objects can be serialized easily and re-used in other sessions without any
> concept of "attaching them" etc...
> 
> As a result, you can easily move objects from one engine to another:
>
> ```python
> with engine1:
>     task = Task.get(1) # SELECT * FROM tasks WHERE id=1
> with engine2:
>     task.insert() # INSERT INTO tasks (id, ...) VALUES (1, ...)
> ```

### Handling unknown columns

By default, unknown columns (ones which are not mapped), will be set as attributes on the object.
Dirty attributes which are not mapped will also be saved in DML statements.

You can thus create models with no mapped columns. However, models require a primary key to function properly.
One will be automatically added when non are defined (named "id").

When no columns are mapped, `SELECT *` is used.

```python
class Task(Model):
    table = "tasks"

with engine:
    task = Task.find_all(done=False) # SELECT * FROM tasks WHERE done = false
    print(task.title) # works
    task.title = "renamed task"
    task.save() # UPDATE tasks SET title = 'renamed task' WHERE id = 1
```

> [!TIP]
> `ModelClass.c` is a shorthand to access the mapper columns (`ModelClass.__mapper__.columns`).
> When unknown columns are allowed, you can use any attributes to get a column object to build queries.
>
> ```python
> todos = Task.find_all(Task.c.done==False)
> ```

> [!WARNING]
> You should not disable dirty tracking when allowing unknown columns otherwise setting attributes
> will not result in them being used in DML statements unless they are mapped.  
> When dirty tracking is disabled and you are using unknown attributes, the only way sqlorm keeps
> track of them is through the [`__hydrated_attrs__` attribute](#dehydrating-objects).

You can disallow unknown columns

```python
class MyBaseModel(Model, abc.ABC):
    class Meta:
        allow_unknown_columns = False
```

Or change the default primary key name:

```python
class MyBaseModel(Model, abc.ABC):
    class Meta:
        auto_primary_key = "uid" # or False to disable auto creating primary key
```

### Relationships

Define relationships on models using `Relationship(target_model, target_column, source_column)`:

```python
from sqlorm import Relationship

class Post(Model):
    comments = Relationship("Comment", "post_id") # target model can be a string if not yet defined, default source column is the primary key

class Comment(Model):
    post = Relationship(Post, "id", "post_id", single=True) # use single=True for many-to-one relationships

with engine:
    post = Post.get(1)
    for comment in post.comments: # SELECT executed now to load comments
        print(comment.content)

    comment = Comment.get(1)
    print(comment.post.slug) # SELECT executed now to load post
```

> [!NOTE]
> There are no notion of backref like you may find in other ORMs. I prefer the explicit nature of declaring the relationship both ways

The list of objects when not using single=True is not a normal python list. It can be iterated over and accessed through brackets.
However, it only contains 2 methods to modify the list: append and remove. These methods will set the attribute on the related object.
It will **not** save the object.

```python
with engine:
    post = Post.get(1)
    comment = Comment(content="hello")
    post.comments.append(comment) # set comment.post_id
    comment.save()
```

By default, relationships are always lazy loaded. Eager loading is possible by setting `lazy=False` or using `with_rels` in finder methods.

```python
class Comment(Model):
    post = Relationship("Post", "id", "post_id", single=True, lazy=False) # add lazy=False

with engine:
    c = Comment.get(1) # SELECT comments.*, posts.id as posts__id, posts.slug as posts__slug, ... FROM comments LEFT JOIN posts ON posts.id = comments.post_id
    c.post.slug # no statement executed, already loaded

    post = Post.get(1, with_rels=["comments"]) # will load comments through the composite row mechanism
    for comment in post.comments: # no statement executed, already loaded
        # ...
```

> [!TIP]
> Use `with_rels=False` to prevent eager loading relationships

Sometimes you want to query through relationships but not load the related objects. Use `with_joins` similarly as `with_rels` but it will
only add the join clauses. You can also access attributes through the relationship object to get properly table aliased columns:

```python
comments = Comment.find_all(Comment.post.slug=="slug", with_joins=["post"]) # SELECT comments.* FROM comments LEFT JOIN posts ON posts.id = comments.post_id WHERE posts.slug == 'slug'
comments = Comment.query(Comment.select_from(with_joins=[Comment.post]).where(Comment.post.slug=="slug")) # alternative
```

> [!TIP]
> The join clause is customizable using `join_type` and `join_condition`
>
> ```python
> class Post(Model):
>     comments = Relationship("Comment", "post_id",
>         join_condition="{target_alias}.post_id = {source_alias}.id AND NOT {target_alias}.archived") # an sql template with 2 special locals: target_alias and source_alias
> 
> class Comment(Model):
>     post = Relationship(Post, "id", "post_id", single=True, join_type="INNER JOIN")
> ```

> [!NOTE]
> There are no support for many-to-many relationships for now

> [!WARNING]
> sqlorm does not handle deleting related objects when the parent is deleted. Use cascading rules in your table definitions.

### Binding models to engines

Unless bound to a specific engine, models will need a session context to execute statements.
When bound to an engine, a non-commiting session is automatically created to execute statements.

To bind models to an engine, create a new base model class using `Model.bind()`:

```python
Model = Model.bind(engine)

class Task(Model):
    pass

tasks = Task.find_all() # works without a session context
```

You can also bind a model to an engine by simply setting its `__engine__` class attribute.

> [!NOTE]
> While binding engine to models is possible, it is not the preferred way to use sqlorm

### Model registry

When defining models using the `Model` class, the model classes are registered in a model registry
available under `Model.__model_registry__`.

Using `Model.bind()` will create a new registry only for the subclasses of this new base class.

You can reference models in your sql methods:

```python
class Post(Model):
    def list_comments(self):
        "SELECT * FROM {Comment.table} WHERE {Comment.post_id} = %(self.id)"

class Comment(Model):
    pass
```

### Eager and lazy loading columns

Sometime you want to load additional data as part of the querying of objects and other times you want
to delay loading additional data to when it's actually needed.

Lazy and eager loading mechanism only apply when using `select_from()`, `find_all()`, `find_one()` and `get()`.

```python
class Task(Model):
    lazy_column = Column(lazy=True)

    # create lazy groups: all columns of a group are loaded together when one is accessed
    grouped_lazy_column1 = Column(lazy="group1")
    grouped_lazy_column2 = Column(lazy="group1")


with engine:
    task = Task.get(1)
    # lazy columns are not loaded, they weren't included in the select statement
    task.lazy_column # SELECT lazy_column FROM tasks WHERE id=1
    task.grouped_lazy_column1 # SELECT grouped_lazy_column1, grouped_lazy_column2 FROM tasks WHERE id=1
    task.grouped_lazy_column2 # no select, already loaded
```

You can eager load lazy fields using `with_lazy` in query methods:

```python
with engine:
    task = Task.get(1, with_lazy=True)
    task = Task.get(1, with_lazy=["group1"]) # only load some lazy items (column or group names)
```

### Column types



## SQL utilities

sqlorm provides the `SQL` class to quickly and easily compose SQL statements.

### Composable pieces of parametrized SQL

An `SQL` object is just a piece of SQL that can be combined with other `SQL` objects to build a statement.

```python
from sqlorm import SQL

# combine pieces of sql
q = SQL("SELECT * FROM table")
q = SQL("SELECT", "*", "FROM", "table")
q = SQL("SELECT *") + SQL("FROM table")
```

Use `SQL.Param` to embed parameters in your queries:

```python
with engine as tx:
    q = SQL("SELECT * FROM table WHERE id =", SQL.Param(1))
    task = tx.fetchone(q)
```

> [!NOTE]
> Parameters automatically use the apropriate paramstyle placeholder defined by the underlying DBAPI driver (default is "qmark").

Use `.render()` to render an SQL object to a tuple containing the sql string and its parameters:

```python
q = SQL("SELECT * FROM table WHERE id =", SQL.Param(1))

assert str(q) == "SELECT * FROM table WHERE id = ?"

stmt, params = q.render()
assert stmt == "SELECT * FROM table WHERE id = ?"
assert params == [1]
```

Use Python operators to combine:

```python
column = SQL("column_name")

s = column == SQL.Param("value") # column_name = ?
s = column > SQL.Param("value") # column_name > ?
s = column == SQL.Param("value") & column != SQL.Param("value2") # (column_name = ? AND column_name != ?)
s = column == SQL.Param("value") | column == SQL.Param("value2") # (column_name = ? OR column_name = ?)
s = ~column # NOT column_name
s = SQL.Param("value").in_(column) # ? IN column
```

Or any custom sql operators:

```python
s = column.op("MATCH", SQL.Param("my text"))
```

Calling methods on `SQL` objects can be used to build full SQL statements in pure python using method chaining.

`sql_object.keyword(*args)` is the same as `sql_object + SQL("KEYWORD", *args)` (underscores in keyword are replaced by spaces and trimmed)

```python
q = SQL().select("*").from_("table").where(column == SQL.Param("value")).order_by(column)
# is the same as
q = SQL("SELECT", "*", "FROM", "table", "WHERE", column == SQL.Param("value"), "ORDER BY", column)
```

Useful shortcuts:

- `SQL.select()` instead of `SQL().select()`
- `SQL.insert_into()` instead of `SQL().insert_into()`
- `SQL.update()` instead of `SQL().update()`
- `SQL.delete_from()` instead of `SQL().delete_from()`

### Handling list of SQL pieces

Use `SQL.List` to manage lists of `SQL` objects. A few subclasses exists:

 - `SQL.List` renders a comma separated list of the rendered items
 - `SQL.Tuple` renders a parentheses enclosed comma separated list of the rendered items
 - `SQL.And` and `SQL.Or` renders a parentheses enclosed list separated by the boolean operator of the rendered items (this is used when combining using `&` and `|`)

```python
q = SQL.select(SQL.List(["col1", "col2"])).from_("table") # SELECT col1, col2 FROM table

q = SQL.insert_into("table", SQL.Tuple(["col1", "col2"])).values(SQL.Tuple([SQL.Param("value1"), SQL.Param("value2")])) # INSERT INTO table (col1, col2) VALUES (?, ?)

l = SQL.List()
l.append("col1") # SQL.List objects are subclasses of python's list
```

### Handling columns

`SQL.Col` and `SQL.Cols` can be used to respectively represent a column and manage a list of columns:

```python
q = SQL.select(SQL.Cols(["col1", "col2"], table="alias")).from_("table AS alias") # SELECT alias.col1, alias.col2 FROM table AS alias

q = SQL.select(SQL.Cols(["col1", "col2"], prefix="prefix_")).from_("table") # SELECT col1 AS prefix_col1, col2 AS prefix_col2 FROM table

q = SQL.select(SQL.Cols([SQL.Col("col1", prefix="prefix_"), SQL.Col("col2", alias="colalias")])).from_("table") # SELECT col1 AS prefix_col1, col2 AS colalias FROM table

columns = SQL.Cols(["col1", "col2", "col3"])
prefixed_cols = columns.prefixed("prefix_") # returns a new column list with all column prefixed
cols_with_table = columns.aliased_table("table") # returns a new column list with all column specifying the table name
s = columns["col1"] == SQL.Param("value") # access individual columns
s = columns.col2 == SQL.Param("value")
```

### Easily generate insert and update statements

```python
with engine as tx:
    tx.execute(SQL.insert("table", {"col1": "value1"}))
    tx.execute(SQL.update("table", {"col1": "value1"}).where("id =", SQL.Param(1)))
```

### Call SQL functions

Use `SQL.funcs.function_name()` where function_name is the SQL function name to render SQL function calls. All arguments are automatically wrapped in `SQL.Param` unless they are already an `SQL` object.

```python
q = SQL("SELECT * FROM table WHERE col =", SQL.funcs.lower("value"))
q = SQL("SELECT", SQL.funcs.count(SQL("*")), "FROM table")
```

### Templates

Templates are the underlying mechanism of sql functions.

 - code in curly braces will be evaluated and be inserted in the final statement as is
 - code in python format markers will be evaluated and inserted as parameters

```python
from sqlorm import SQLTemplate

tpl = SQLTemplate("SELECT * FROM {table} WHERE created_at >= %(created_at)s")
stmt, params = tpl.render({"table": "my_table", "created_at": "2024-01-01"})
```

## Mapping any class

Under the hood, models subclassing `Model` and other classes used as model are mapped using a `Mapper`.
A mapper contains all the information needed to hydrate/dehydrate an object as well as utility methods
to create SQL statements to fetch/save the data.

### Defining mappers

Create a mapper and map some columns:

```python
from sqlorm.mapper import Mapper, Column

class Task:
    pass

mapper = Mapper(Task, "tasks")
mapper.map({
    "id": Column("id", primary_key=True),
    "title": Column("title"),
    "done": Column("completed") # in this example, the column is named "completed" but the attribute is named "done"
})
```

> [!NOTE]
> The `Column` class used here is not the same as the one used in the models section.  
> `Column` used in the model definitions is a subclass of the mapper's `Column` with support for property accessors and dirty attributes.

By default, unknown columns will be accepted and set as attributes. This can be changed using `allow_unknown_columns=False` in mapper's constructor.

Mappers can be created from any classes, in which case annotations will be used:

```python
from sqlorm import PrimaryKey

class Task:
    id: PrimaryKey[int]
    title: str
    done: bool # no column alias possible in this way (but possible when subclassing Model)

mapper = Mapper.from_class(Task)
```

> [!TIP]
> Get the mapper associated to models using `Mapper.from_class(ModelClass)` or `ModelClass.__mapper__`

### Hydrate objects

Use row data to populate an object.

`hydrate_new()` is used to create new object and hydrate it, `hydrate()` for existing objects.

```python
data = {"id": 1, "title": "task", "completed": True}

task = mapper.hydrate_new(data)

task = Task()
mapper.hydrate(task, data) # hydrate an existing object
```

> [!IMPORTANT]
> When hydrating objects, `__init__()` will be bypassed as well as any custom `__setattr__`.

By default, unmapped columns will be used and set as attributes. To prevent this, use `allow_unknown_columns=False` in the `Mapper` constructor or
`with_unknown=False` in hydrate methods.

Attributes that have been hydrated are available under the object's `__hydrated_attrs__` attribute.

### Dehydrating objects

Extract data from an object and construct row data.

```python
task = Task()
task.id = 1
task.title = "task"
task.done = True
data = mapper.dehydrate(task)
```

By default, if unknown columns are allowed, dehydration will also take into account all unknown attributes from `__hydrated_attrs__`.
Use `dehydrate(..., with_unknown=False)` to prevent this.

When creating update statements, use `with_primary_key=False` to not include the primary key as part of the dehydration process.

### Generating SQL statements

A few useful methods to generate statementes:

- `select_from()` to generate a select statement with the columns and table part already filled. The column list is generated using the mapping information and falls back to "*" when no columns are provided.
- `select_by_pk()` to generate a select statement with a where condition on the primary key
- `insert()` to generate the insert statement for an object
- `update()` to generate the update statement
- `delete()` to generate the delete statement

```python
with engine as tx:
    tasks = tx.fetchhydrated(mapper, mapper.select_from())
    todos = tx.fetchhydrated(mapper, mapper.select_from().where("not done"))

    task1 = tx.fetchhydrated(mapper, mapper.select_from().where("id = 1")).first()
    task1 = tx.fetchhydrated(mapper, mapper.select_by_pk(1)).first()
    
    task = Task()
    tx.execute(mapper.insert(task))

    task = Task()
    task.id = 1
    task.title = "task"
    tx.execute(mapper.update(task))

    tx.execute(mapper.delete(task))
```

The mapper column list `mapper.columns` is an [`SQL.Cols`](#manage-list-of-sql-pieces) object which means it can be used to generate sql conditions:

```python
with engine as tx:
    todos = tx.fetchhydrated(mapper, mapper.select_from().where(mapper.columns.done==False))
```

## Managing the schema

### Creating tables from models

Use `create_table()` to create the table associated to a model or mapper.

```python
from sqlorm import create_table

class Task:
    id: PrimaryKey[int]
    title: str
    done: bool

with engine:
    create_table(Task)
```

Use `create_all()` to create all tables for models in a registry. If no registry is provided, `Model.__model_registry__` is used.

```python
from sqlorm import create_all

class Task(Model):
    id: PrimaryKey[int]
    title: str
    done: bool

with engine:
    create_all()
```

You can check if a table exists first and prevent executing create table statements for existing tables using `check_missing=True`.

> [!TIP]
> If you have bound your models to an engine, pass the model registry to `create_all()`:
> 
> ```python
> Model = Model.bind(engine)
> 
> # ... define models
> 
> with engine:
>     create_all(Model.__model_registry__)
> ```

### Migrations

sqlorm includes a simple one-way migration system. It will execute a list of sql or python files in order.

Create a *migrations* folder with *.sql* or *.py* files prefixed with a version number (1 or more number) followed by an underscore and a name.

```
migrations/
    000_init.sql
    001_add_indexes.sql
    002_seed_data.py
```

Use `migrate()` to execute these files in order. If no errors are raised during the run, the latest version is saved in a *schema_version* table in the database.
It will be used as starting point for the next time you run `migrate()` (unless `use_schema_version=False` is used).

```python
from sqlorm import migrate

with engine:
    migrate()
```

> [!TIP]
> The *transactions* folder is resolved relative to the working directory. Provide an alternative path as first argument to `migrate()`.

SQL files can contain multiple statements.

In python files, use `get_current_session()` to create a transaction context:

```python
# 002_seed_data.py
from sqlorm import get_current_session

with get_current_session() as tx:
    tx.execute("...")
```

### Initializing the database

Use `init_db()` to quickly initialize a database, either using `create_all(check_missing=True)` if no migrations exist or migrating the schema to the latest version.

```python
from sqlorm import init_db

with engine:
    init_db()
```

## Database specific

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

## Acknowledgements

sqlorm is inspired by the many great ORMs that already exists in Python and other languages.

Big shoutout to [SQL Alchemy](https://www.sqlalchemy.org/).

On a side note, I had explored a similar approach in PHP in 2010 in my [classql](https://github.com/maximebf/classql) project.