# sqlorm

**SQL-focused Python ORM**

![GitHub Actions Workflow Status](https://img.shields.io/github/actions/workflow/status/hyperflask/sqlorm/python.yml?branch=main) ![Codecov](https://img.shields.io/codecov/c/github/hyperflask/sqlorm)

ORMs and developers often end up in a love/hate relationship. The advantages of ORMs (the mapping of objects) can be negated by their compromises (have to learn their pseudo sql syntax, less control over the generated sql, low performance).

sqlorm intends to provide a solution where sql stays front and center and where the behavior of the ORM is what you expect and no more. SQL is seamlessly integrated through functions and model methods using python doc strings.

- Thin layer on top of [DBAPI 2.0](https://peps.python.org/pep-0249/)
- Use SQL as usual
- Utilities to help you build composable SQL queries
- ActiveRecord-style ORM that is mostly direct row to object
- Supports optional model definition via annotations
- Predictable SQL generation from models
- Supports relationships
- Built-in SQL based migration system
- Low coupling between different parts of the library which can be used directly with DBAPI Connections
- Easy to understand code

[Read the documentation](https://hyperflask.github.io/sqlorm)

See [Flask-SQLORM](https://github.com/hyperflask/flask-sqlorm) for a Flask integration.

## Installation

    pip install sqlorm-py

## Example

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
        "SELECT WHERE not done"

    def toggle(self):
        "UPDATE SET done = not done WHERE SELF RETURNING *"

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
