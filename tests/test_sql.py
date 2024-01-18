from sqlorm.sql import SQL, ParameterCollector, render, ParametrizedStmt, split_sql_statements


def test_sql():
    assert str(SQL("col", "=", "value")) == "col = value"
    assert str(SQL("col") + SQL("=value")) == "col =value"
    assert (SQL("col") == "foo").render() == ("col = ?", ["foo"])
    assert str(SQL("col") != "foo") == "col != ?"
    assert str(SQL("col") < "foo") == "col < ?"
    assert str(SQL("col") <= "foo") == "col <= ?"
    assert str(SQL("col") > "foo") == "col > ?"
    assert str(SQL("col") >= "foo") == "col >= ?"
    assert str(~SQL("col")) == "NOT col"
    assert str(SQL("col").in_(SQL("foo"))) == "col IN foo"
    assert str(SQL("true") & SQL("true")) == "( true AND true )"
    assert str(SQL("true") | SQL("true")) == "( true OR true )"


def test_param_collector():
    params = ParameterCollector(paramstyle="pyformat")
    stmt = []
    stmt.append(params.add("foo"))
    stmt.append(params.add("bar", "hello"))
    stmt.append(params.add("buz"))
    assert stmt == ["%(param_1)s", "%(hello)s", "%(param_2)s"]
    assert params.values == {"param_1": "foo", "hello": "bar", "param_2": "buz"}
    assert params.get("param_1") == "foo"
    assert params.index(1) == "bar"

    params = ParameterCollector(paramstyle="named")
    stmt = []
    stmt.append(params.add("foo"))
    stmt.append(params.add("bar", "hello"))
    stmt.append(params.add("buz"))
    assert stmt == [":param_1", ":hello", ":param_2"]
    assert params.values == {"param_1": "foo", "hello": "bar", "param_2": "buz"}

    params = ParameterCollector(paramstyle="qmark")
    stmt = []
    stmt.append(params.add("foo"))
    stmt.append(params.add("bar"))
    assert stmt == ["?", "?"]
    assert params.values == ["foo", "bar"]
    assert params.names == ["param_1", "param_2"]
    assert params.get("param_1") == "foo"
    assert params.index(1) == "bar"

    params = ParameterCollector(paramstyle="numeric")
    stmt = []
    stmt.append(params.add("foo"))
    stmt.append(params.add("bar"))
    assert stmt == [":1", ":2"]
    assert params.values == ["foo", "bar"]


def test_sql_params():
    assert str(SQL.Param("hello")) == "?"
    assert SQL.Param("hello").as_style("pyformat") == "%(param)s"
    assert SQL.Param("hello", "foo").as_style("pyformat") == "%(foo)s"
    assert SQL.Param("hello").as_style("qmark") == "?"
    assert SQL.Param("hello").as_style("numeric") == ":0"

    stmt, params = render(SQL.Param("hello"))
    assert stmt == "?"
    assert params == ["hello"]


def test_sql_id():
    assert str(SQL.Id("table")) == "table"
    assert str(SQL.Id("table", "alias")) == "table AS alias"
    assert SQL.Id("table", "alias").alias_or_name == "alias"
    assert str(SQL.Id("table").aliased("alias")) == "table AS alias"

    assert str(SQL.Id(SQL.Id("table"))) == "table"


def test_sql_column():
    assert str(SQL.Col("col")) == "col"
    assert SQL.Col("col").alias_or_name == "col"
    assert str(SQL.Col("col", "table")) == "table.col"

    assert str(SQL.Col("col", prefix="prefix_")) == "col AS prefix_col"
    assert SQL.Col("col", prefix="prefix_").alias_or_name == "prefix_col"
    assert str(SQL.Col("col", "table", prefix="prefix_")) == "table.col AS prefix_col"

    assert str(SQL.Col("col", alias="alias")) == "col AS alias"
    assert SQL.Col("col", alias="alias").alias_or_name == "alias"
    assert str(SQL.Col("col", "table", alias="alias")) == "table.col AS alias"

    assert str(SQL.Col("col").aliased_table("table")) == "table.col"
    assert str(SQL.Col("col").aliased("alias")) == "col AS alias"
    assert str(SQL.Col("col").prefixed("prefix_")) == "col AS prefix_col"

    q = "SELECT COUNT(*) FROM children"
    c = SQL.ColExpr(q, "nb_children")
    assert str(c) == f"({q}) AS nb_children"
    assert str(c.aliased_table("table")) == f"({q}) AS nb_children"
    assert str(c.aliased("alias")) == f"({q}) AS alias"
    assert str(c.prefixed("prefix_")) == f"({q}) AS prefix_nb_children"

    assert str(SQL.Col(SQL.Col("col", "table"), prefix="prefix_")) == "table.col AS prefix_col"
    assert str(SQL.Col(SQL.Id("col"), prefix="prefix_")) == "col AS prefix_col"


def test_sql_column_list():
    cols = SQL.Cols(["col1", "col2", SQL.Col("col3")])
    assert all([isinstance(c, SQL.Col) for c in cols])
    assert str(cols) == "col1 , col2 , col3"
    assert cols.names == ["col1", "col2", "col3"]
    assert str(cols.aliased_table("table")) == "table.col1 , table.col2 , table.col3"
    assert str(cols.prefixed("prefix_")) == "col1 AS prefix_col1 , col2 AS prefix_col2 , col3 AS prefix_col3"
    assert cols.get("col1").name == "col1"
    
    assert str(SQL.Cols([])) == "*"
    assert str(SQL.Cols([], wildcard=False)) == ""
    assert str(SQL.Cols(["col1"], wildcard=True)) == "*"
    assert str(SQL.Cols([], table="table")) == "table.*"
    assert str(SQL.Cols([], prefix="prefix_")) == "*"


def test_sql_list():
    assert str(SQL.Tuple(["col1", "col2"])) == "( col1 , col2 )"

    assert render(SQL.And([SQL("col1") == SQL.Param("value1"), SQL("col2") == SQL.Param("value2")]))[0] == \
        "( col1 = ? AND col2 = ? )"
    
    assert render(SQL.And([SQL("col1") == SQL.Param("value1"), SQL.Or([SQL("col2") == SQL.Param("value2"), SQL("col3") == SQL.Param("value3")])]))[0] == \
        "( col1 = ? AND ( col2 = ? OR col3 = ? ) )"


def test_query_builder():
    stmt, params = render(SQL.insert_into("table", SQL.Tuple(["col1", "col2"])).values(SQL.Tuple([SQL.Param("value1"), SQL.Param("value2")])))
    assert stmt == "INSERT INTO table ( col1 , col2 ) VALUES ( ? , ? )"
    assert params == ["value1", "value2"]

    assert SQL.insert("table", {"id": 1, "col": "foo"}).render() == ("INSERT INTO table ( id , col ) VALUES ( ? , ? )", [1, "foo"])
    assert SQL.insert("table", "SELECT * FROM other").render() == ("INSERT INTO table SELECT * FROM other", [])

    assert SQL.update("table", {"col": "foo"}).render() == ("UPDATE table SET col = ?", ["foo"])

    assert SQL.delete_from("table").render() == ("DELETE FROM table", [])
    

def test_sql_funcs():
    assert str(SQL.funcs.count(SQL.Col("*"))) == "COUNT ( * )"
    assert str(SQL.funcs.lower("username")) == "LOWER ( ? )"


def test_render():
    assert render("col = ?", [1]) == ("col = ?", [1])
    assert render("col = %(col)s", {"col": "val"}) == ("col = %(col)s", {"col": "val"})
    assert render(SQL("col =", SQL.Param(1))) == ("col = ?", [1])
    assert render(SQL("col =", SQL.Param(1)), paramstyle="named") == ("col = :param_1", {"param_1": 1})
    assert render(SQL("col = %(col)s"), {"col": "val"}) == ("col = %(col)s", {"col": "val"})
    assert render(SQL("col = ?"), [1]) == ("col = ?", [1])
    assert render(SQL("col = ? AND col2 =", SQL.Param("val2")), ["val"]) == ("col = ? AND col2 = ?", ["val", "val2"])

    class S:
        def __init__(self, stmt, params=None):
            self.stmt = stmt
            self.params = params
        def __sql__(self):
            if self.params:
                return self.stmt, self.params
            return self.stmt
        
    assert render(S("col = ?")) == ("col = ?", None)
    assert render(S("col = ?", [1])) == ("col = ?", [1])

    assert render(SQL("col =", SQL.ParamPlaceholder("foo")), {"foo": "bar"}, paramstyle="qmark") == ("col = ?", ["bar"])

    s = ParametrizedStmt("SELECT * FROM table WHERE id = ?", [1])
    assert str(s) == "SELECT * FROM table WHERE id = ?"
    assert render(s) == ("SELECT * FROM table WHERE id = ?", [1])


def test_split_statements():
    sql = "SELECT * FROM table WHERE ';'; INSERT INTO table"
    stmts = split_sql_statements(sql)
    assert len(stmts) == 2
    assert stmts[0] == "SELECT * FROM table WHERE ';'"
    assert stmts[1] == "INSERT INTO table"