# SQL functions

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