import json
import pickle
import datetime


PYTHON_TYPES_MAP = {}


class SQLType:
    @classmethod
    def from_pytype(cls, python_type):
        if isinstance(python_type, SQLType):
            return python_type
        if isinstance(python_type, str):
            return SQLType(python_type)
        if python_type in PYTHON_TYPES_MAP:
            return PYTHON_TYPES_MAP[python_type]
        return SQLType(getattr(python_type, "__sqltype__", python_type.__name__))

    def __init__(self, sql_type, loader=None, dumper=None):
        self.sql_type = sql_type
        self.loader = loader
        self.dumper = dumper

    def __eq__(self, other):
        if isinstance(other, SQLType):
            return other.sql_type == self.sql_type
        if isinstance(other, str):
            return self.sql_type == other
        return SQLType.from_pytype(other).sql_type == self.sql_type
    
    def __str__(self):
        return self.sql_type


Integer = SQLType("integer")
Decimal = SQLType("decimal")
Varchar = SQLType("varchar")
Text = SQLType("text")
Boolean = SQLType("boolean")
Date = SQLType("date")
Time = SQLType("time")
DateTime = SQLType("datetime")
JSON = SQLType("json", json.loads, json.dumps)
Pickle = SQLType("text", pickle.loads, pickle.dumps)

PYTHON_TYPES_MAP.update({
    int: Integer,
    float: Decimal,
    str: Text,
    bool: Boolean,
    datetime.date: Date,
    datetime.time: Time,
    datetime.datetime: DateTime,
    dict: JSON
})

__all__ = (
    "SQLType",
    "Integer",
    "Decimal",
    "Varchar",
    "Text",
    "Boolean",
    "Date",
    "DateTime",
    "JSON",
    "Pickle"
)