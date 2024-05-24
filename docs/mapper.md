# Mapping any class

Under the hood, models subclassing `Model` and other classes used as model are mapped using a `Mapper`.
A mapper contains all the information needed to hydrate/dehydrate an object as well as utility methods
to create SQL statements to fetch/save the data.

## Defining mappers

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

## Hydrate objects

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

## Dehydrating objects

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

## Generating SQL statements

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

The mapper column list `mapper.columns` is an [`SQL.Cols`](sql-utilities.md#manage-list-of-sql-pieces) object which means it can be used to generate sql conditions:

```python
with engine as tx:
    todos = tx.fetchhydrated(mapper, mapper.select_from().where(mapper.columns.done==False))
```
