import typing as t
from collections import namedtuple


def render(stmt, params=None, dbapi=None, paramstyle=None):
    """Renders the provided sql statement and returns a tuple (sql, params)
    """
    if not dbapi and not paramstyle and isinstance(params, dict):
        paramstyle = "pyformat"
    elif not paramstyle and dbapi:
        paramstyle = dbapi.paramstyle
    elif not paramstyle:
        paramstyle = "qmark"

    if isinstance(stmt, SQLStr):
        collector = ParameterCollector(params, paramstyle)
        stmt = stmt._render(collector)
        params = collector.values
    elif hasattr(stmt, "__sql__"):
        stmt = stmt.__sql__()
        if isinstance(stmt, tuple):
            stmt, params = stmt
    return str(stmt), params
    

class ParametrizedStmt(namedtuple("_ParametrizedStmt", ["stmt", "params"])):
    """A simple class to hold together an SQL statement and its parameters"""
    def __sql__(self):
        return self.stmt, self.params
    
    def __str__(self):
        return self.stmt


class SQLStr:
    """The base for our SQL pieces"""

    def render(self):
        """Renders this piece of sql and returns a tuple (sql_stmt, params)"""
        collector = ParameterCollector()
        stmt = self._render(collector)
        return str(stmt), collector.values
    
    def _render(self, params):
        raise NotImplementedError()
    
    def __sql__(self):
        return self.render()[0]
    
    def __str__(self):
        return self.render()[0]
    
    def op(self, operator, other):
        if not isinstance(other, SQLStr):
            other = SQL.Param(other)
        return SQL(self, operator, other)
    
    def __and__(self, other):
        return And([self, other])

    def __or__(self, other):
        return Or([self, other])
    
    def __add__(self, other):
        return SQL(self, other)
    
    def __invert__(self):
        return SQL("NOT", self)

    def __lt__(self, other):
        return self.op("<", other)
    
    def __le__(self, other):
        return self.op("<=", other)
    
    def __eq__(self, other):
        return self.op("=", other)
    
    def __ne__(self, other):
        return self.op("!=", other)
    
    def __gt__(self, other):
        return self.op(">", other)
    
    def __ge__(self, other):
        return self.op(">=", other)
    
    def in_(self, other):
        return self.op("IN", other)
    
    def notin(self, other):
        return self.op("NOT IN", other)
    
    def like(self, other):
        return self.op("LIKE", other)
    
    def ilike(self, other):
        return self.op("ILIKE", other)


class SQL(SQLStr):
    """Represents a composable piece of SQL
    """

    def __init__(self, *parts):
        self.parts = list(parts)

    def _render(self, params):
        stmt = []
        for part in self.parts:
            if isinstance(part, SQLStr):
                stmt.append(str(part._render(params)))
            elif part:
                stmt.append(str(part))
        return " ".join(filter(bool, stmt))
    
    def __getattr__(self, name):
        """Allows you to do some query builder semantics
        Example: SQL().select("*").from_("table").where("condition").limit(1)
        """
        return lambda *p: SQL(*list(self.parts + [name.replace("_", " ").upper().strip()] + list(p)))
    
    @staticmethod
    def select(*args):
        return SQL("SELECT", *args)
    
    @staticmethod
    def insert_into(*args):
        return SQL("INSERT INTO", *args)
    
    @staticmethod
    def insert(table, values):
        if not values:
            return SQL("INSERT INTO", table, "DEFAULT VALUES")
        if isinstance(values, (str, SQLStr)):
            return SQL("INSERT INTO", table, values)
        values_sql = Tuple([v if isinstance(v, SQLStr) else Parameter(v, c) for c, v in values.items()])
        return SQL("INSERT INTO", table, Tuple(values.keys()), "VALUES", values_sql)
    
    @staticmethod
    def update(table, values=None):
        if values is None:
            return SQL("UPDATE", table)
        values_sql = List([SQL(c, "=", v if isinstance(v, SQLStr) else Parameter(v, c)) for c, v in values.items()])
        return SQL("UPDATE", table, "SET", values_sql)
    
    @staticmethod
    def delete_from(table):
        return SQL("DELETE FROM", table)
    
    @staticmethod
    def case(whens, else_=None):
        return SQL("CASE", List([SQL("WHEN", w, "THEN", t) for w, t in whens.items()], ""), SQL("ELSE", else_) if else_ else "", "END CASE")


class Parameter(SQLStr):
    """A statement parameter contains a value that won't be outputted in the SQL statement
    """

    def __init__(self, value, name=None):
        self.value = value
        self.name = name

    def as_style(self, style="qmark"):
        name = "param" if self.name is None and style != "numeric" else self.name
        return format_param(name, style)
    
    def _render(self, params):
        return params.add(self.value, self.name)
    

class ParameterPlaceholder(Parameter):
    def __init__(self, name):
        self.name = name

    def _render(self, params):
        return format_param(self.name, params.paramstyle)


class Identifier(SQLStr):
    """An SQL identifier that can be aliased"""

    def __init__(self, name, alias=None):
        if isinstance(name, Identifier):
            alias = alias or name.alias
            name = name.name
        self.name = name
        self.alias = alias

    @property
    def alias_or_name(self):
        return self.alias or self.name

    def _render(self, params):
        if self.alias:
            return f"{self.name} AS {self.alias}"
        return self.name

    def aliased(self, alias):
        return Identifier(self.name, alias)


class Column(Identifier):
    """Represents a column with eventually the table identifier and an alias
    """

    def __init__(self, name, table=None, prefix=None, alias=None):
        if isinstance(name, Column):
            table = table or name.table
            prefix = prefix or name.prefix
            alias = alias or name.alias
            name = name.name
        elif isinstance(name, Identifier):
            alias = alias or name.alias
            name = name.name
        self.name = name
        self.table = table
        self.prefix = prefix
        self.alias = alias

    @property
    def alias_or_name(self):
        name = self.alias or self.name
        if self.prefix:
            return f"{self.prefix}{name}"
        return name

    def _render(self, params):
        name = f"{self.table}.{self.name}" if self.table else self.name
        alias = self.alias
        if self.prefix:
            alias = f"{self.prefix}{self.alias or self.name}"
        if alias:
            return f"{name} AS {alias}"
        return name

    def aliased_table(self, table):
        """Returns a new Column object where the table part is set"""
        return Column(self.name, table, self.prefix, self.alias)

    def aliased(self, alias):
        """Returns a new Column object with an alias"""
        return Column(self.name, self.table, self.prefix, alias)

    def prefixed(self, prefix):
        """Returns a new Column object with an alias that is the name prefixed"""
        return Column(self.name, self.table, prefix, self.alias)
    

class ColumnExpr(Column):
    """A virtual column that is an expression
    """
    def __init__(self, expr, alias, table=None, prefix=None):
        super().__init__(alias, table=table, prefix=prefix)
        self.expr = expr

    def _render(self, params):
        expr = SQL(self.expr)._render(params)
        alias = self.name
        if self.prefix:
            alias = f"{self.prefix}{alias}"
        return f"({expr}) AS {alias}"

    def aliased_table(self, table):
        """Returns a new ColumnExpr object with a table alias"""
        return ColumnExpr(self.expr, self.name, table, self.prefix)

    def aliased(self, alias):
        """Returns a new ColumnExpr object with an alias"""
        return ColumnExpr(self.expr, alias, self.table, self.prefix)

    def prefixed(self, prefix):
        """Returns a new ColumnExpr object with an alias that is the name prefixed"""
        return ColumnExpr(self.expr, self.name, self.table, prefix)


class ColumnList(SQLStr, list):
    """A list of Column object with some easy accessors and utility methods
    When converted to str, will print a comma separated list of the columns
    """
    column_class = Column

    def __init__(self, columns, table=None, prefix=None, wildcard=None):
        super().__init__()
        self.table = table
        self.prefix = prefix
        self.wildcard = wildcard
        for col in columns:
            if isinstance(col, Column) and table or prefix:
                if table:
                    col = col.aliased_table(table)
                if prefix:
                    col = col.prefixed(prefix)
            elif not isinstance(col, Column):
                col = self.column_class(col)
            self.append(col)

    def _render(self, params):
        if self.wildcard or self.wildcard is None and not self:
            return Column("*", self.table)._render(params)
        if not self:
            return ""
        return List(self)._render(params)
    
    def aliased_table(self, alias):
        return ColumnList(self, alias, self.prefix, self.wildcard)
    
    def prefixed(self, prefix):
        return ColumnList(self, self.table, prefix, self.wildcard)
    
    def get(self, name):
        for col in self:
            if col.name == name:
                return col
    
    def __getitem__(self, name):
        if isinstance(name, str):
            col = self.get(name)
            if not col:
                raise KeyError()
            return col
        return super().__getitem__(name)
    
    def __contains__(self, name):
        return bool(self.get(name))
    
    def __getattr__(self, name):
        return self[name]
    
    @property
    def names(self):
        return [c.name for c in self]
        
    def as_dict(self):
        return {c.name: c for c in self}


class List(SQLStr, list):
    """A list of SQL items"""

    def __init__(self, items=None, joinstr=",", startstr="", endstr=""):
        super().__init__(items)
        self.joinstr = joinstr
        self.startstr = startstr
        self.endstr = endstr

    def _render(self, params):
        out = SQL()
        if self.startstr:
            out.parts.append(self.startstr)
        for i, part in enumerate(self):
            out.parts.append(part)
            if i + 1 < len(self):
                out.parts.append(self.joinstr)
        if self.endstr:
            out.parts.append(self.endstr)
        return out._render(params)
    

class And(List):
    def __init__(self, items):
        super().__init__(items, "AND", "(", ")")
    

class Or(List):
    def __init__(self, items):
        super().__init__(items, "OR", "(", ")")
    

class Tuple(List):
    def __init__(self, items):
        super().__init__(items, ",", "(", ")")


class Function(SQLStr):
    def __init__(self, name, *args):
        self.name = name.upper()
        self.args = args

    def __call__(self, *args):
        self.args = args
        return self

    def _render(self, params):
        return SQL(self.name, Tuple([Parameter(a) if not isinstance(a, SQLStr) else a for a in self.args]))._render(params)
    

class FunctionFactory:
    def __getattr__(self, name):
        return Function(name)
    
    def __getitem__(self, name):
        return Function(name)


def format_param(name, paramstyle):
    """Format a parameter based on the DBAPI paramstyle
    """
    if paramstyle == "qmark":
        return "?"
    if paramstyle == "format":
        return "%s"
    if paramstyle == "numeric":
        if name is None:
            name = 0
        return f":{name}"
    if name is None:
        name = "param"
    if paramstyle == "named":
        return f":{name}"
    return f"%({name})s"


class ParameterCollectorError(Exception):
    pass


class ParameterCollector:
    """Collect parameters, ensuring they have unique names
    
    Example:
    collector = ParameterCollector()
    value = "value"
    stmt = f"SELECT * FROM table WHERE column = {collector.add(value)}"
    assert stmt == "SELECT * FROM table WHERE column = ?"
    assert collector.values == ["value"]
    """

    def __init__(self, params=None, paramstyle="qmark"):
        self.paramstyle = paramstyle
        self.names = []
        if paramstyle in ("qmark", "numeric", "format"):
            self.values = []
            if isinstance(params, dict):
                self.names.extend(params.keys())
                self.values.extend(params.values())
            elif params:
                self.values.extend(params)
                self.names.extend([f"param_{i+1}" for i in range(len(params))])
        else:
            self.values = {}
            if params and not isinstance(params, dict):
                raise ParameterCollectorError("paramstyle is named, a dict must be provided as params")
            elif params:
                self.names.extend(params.keys())
                self.values.update(params)

    def add(self, value, name: t.Optional[str] = None):
        """Add a parameter (possibly named) and returns its correctly formatted
        placeholder to add in the SQL statement
        """
        if not name or name in self.names:
            if not name:
                name = "param"
            i = 1
            while f"{name}_{i}" in self.names:
                i += 1
            name = f"{name}_{i}"
        self.names.append(name)
        if isinstance(self.values, dict):
            self.values[name] = value
        else:
            self.values.append(value)
            name = len(self.values)
        return format_param(name, self.paramstyle)
    
    def get(self, name):
        """Returns the value of the named parameter"""
        if name not in self.names:
            raise ParameterCollectorError(f"Missing parameter '{name}'")
        if isinstance(self.values, dict):
            return self.values[name]
        return self.values[self.names.index(name)]
    
    def index(self, idx):
        """Returns the value of the parameter at position"""
        if idx < 0 or idx >= len(self.names):
            raise ParameterCollectorError("index out of bound")
        return self.get(self.names[idx])
    
    def __getitem__(self, name):
        return self.get(name)


def split_sql_statements(sql):
    stmts = []
    current = ""
    in_str = False
    for c in sql:
        if c == "'":
            in_str = not in_str
        if c == ";" and not in_str:
            current = current.strip()
            if current:
                stmts.append(current)
            current = ""
        else:
            current += c
    current = current.strip()
    if current:
        stmts.append(current)
    return stmts


SQL.List = List
SQL.And = And
SQL.Or = Or
SQL.Tuple = Tuple
SQL.Param = Parameter
SQL.ParamPlaceholder = ParameterPlaceholder
SQL.Id = Identifier
SQL.Col = Column
SQL.ColExpr = ColumnExpr
SQL.Cols = ColumnList
SQL.Func = Function
SQL.funcs = FunctionFactory()