from psycopg import *
from sqlorm.types import SQLType
from sqlorm.sql import SQL as _SQL, SQLStr


class Array(SQLType):
    def __init__(self, inner_type, loader=None, dumper=None):
        super().__init__(f"{inner_type}[]", loader, dumper)


JSON = SQLType("JSON")
JSONB = SQLType("JSONB")


class SQL(_SQL):
    @staticmethod
    def cast(expr, type):
        if not isinstance(expr, SQLStr):
            expr = SQL.Param(expr)
        return SQL("(", expr, "::", str(type), ")")