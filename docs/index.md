# Introduction

## Installation

    pip install sqlorm-py

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
        "SELECT WHERE not done" # SELECT FROM clause is automatically completed

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

## Acknowledgements

sqlorm is inspired by the many great ORMs that already exists in Python and other languages.

Big shoutout to [SQL Alchemy](https://www.sqlalchemy.org/).

On a side note, I had explored a similar approach in PHP in 2010 in my [classql](https://github.com/maximebf/classql) project.