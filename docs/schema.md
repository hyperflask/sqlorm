# Managing the schema

## Creating tables from models

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

## Migrations

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

## Initializing the database

Use `init_db()` to quickly initialize a database, either using `create_all(check_missing=True)` if no migrations exist or migrating the schema to the latest version.

```python
from sqlorm import init_db

with engine:
    init_db()
```
