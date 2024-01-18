from .sql import SQL, SQLStr
import datetime
import uuid


class EvalBlock(SQLStr):
    """A lazy evaluated piece of SQL. callback will be called at render time"""

    def __init__(self, template, code):
        self.template = template
        self.code = code

    def _render(self, params):
        return eval(self.code, self.template.eval_globals, self.template.locals)
    

class ParametrizedEvalBlock(EvalBlock):
    def _render(self, params):
        return params.add(super()._render(params))
    

class SQLTemplateError(Exception):
    pass


class SQLTemplate(SQL):
    """Similar to f-strings but to generated parametrized statements

    Two types of placeholder are possible:
     - curly braces one, same as f-strings. contains python code that will be evaluated
       the python code should return a string of an SQL-compatible object (eg: Parameter)
     - python format placeholder "%(...)s" where the content is evaluated as python code and wrapped in Parameter

    Available locals in the evaluated python code is provided to SQLTemplate

    Example:
        locals = {"table": "foo", "id": 1}
        tpl = SQLTemplate("SELECT * FROM {table} WHERE id = %(id)s", locals)
        stmt, params = render(tpl)
        assert stmt == "SELECT * FROM foo WHERE id = ?"
        assert params == [1]
    """

    eval_globals = {
        "SQL": SQL,
        "datetime": datetime,
        "uuid": uuid
    }

    eval_blocks = [
        ("{", "}", EvalBlock),
        ("%(", ")s", ParametrizedEvalBlock)
    ]

    def __init__(self, stmt, locals=None):
        self.locals = locals if locals is not None else {}
        i = 0
        eval_block = False
        code = ""
        sql = ""
        sql_parts = []
        nestedc = 0

        while True:
            if i >= len(stmt):
                break

            is_eval = False
            for start, end, eval_callback in self.eval_blocks:
                if (not eval_block or eval_block == start) and len(stmt) >= i+len(start)-1 and stmt[i:i+len(start)] == start:
                    if eval_block:
                        nestedc += 1
                        code += start
                    else:
                        code = ""
                        eval_block = start
                        nestedc = 0
                    i += len(start) - 1
                    is_eval = True
                elif eval_block == start and len(stmt) >= i+len(end)-1 and stmt[i:i+len(end)] == end:
                    if nestedc > 0:
                        nestedc -= 1
                        code += end
                    else:
                        sql_parts.append(sql.strip())
                        sql_parts.append(eval_callback(self, code))
                        sql = ""
                        eval_block = False
                    i += len(end) - 1
                    is_eval = True
                if is_eval:
                    break

            if not is_eval:
                if eval_block:
                    code += stmt[i]
                else:
                    sql += stmt[i]
            i += 1

        if eval_block:
            raise SQLTemplateError("Syntax error: eval block is not closed")
        if sql:
            sql_parts.append(sql.strip())
        super().__init__(*sql_parts)

    def render(self, locals=None):
        _locals = self.locals
        if locals:
            self.locals = dict(self.locals, **locals)
        stmt, params = super().render()
        self.locals = _locals
        return stmt, params