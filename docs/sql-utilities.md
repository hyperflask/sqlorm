# SQL utilities

sqlorm provides the `SQL` class to quickly and easily compose SQL statements.

## Composable pieces of parametrized SQL

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

## Handling list of SQL pieces

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

## Handling columns

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

## Easily generate insert and update statements

```python
with engine as tx:
    tx.execute(SQL.insert("table", {"col1": "value1"}))
    tx.execute(SQL.update("table", {"col1": "value1"}).where("id =", SQL.Param(1)))
```

## Call SQL functions

Use `SQL.funcs.function_name()` where function_name is the SQL function name to render SQL function calls. All arguments are automatically wrapped in `SQL.Param` unless they are already an `SQL` object.

```python
q = SQL("SELECT * FROM table WHERE col =", SQL.funcs.lower("value"))
q = SQL("SELECT", SQL.funcs.count(SQL("*")), "FROM table")
```

## Templates

Templates are the underlying mechanism of sql functions.

 - code in curly braces will be evaluated and be inserted in the final statement as is
 - code in python format markers will be evaluated and inserted as parameters

```python
from sqlorm import SQLTemplate

tpl = SQLTemplate("SELECT * FROM {table} WHERE created_at >= %(created_at)s")
stmt, params = tpl.render({"table": "my_table", "created_at": "2024-01-01"})
```
