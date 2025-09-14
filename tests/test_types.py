from sqlorm.types import SQLType
from sqlorm.types.encrypted import Encrypted
import hashlib


def test_type():
    t = SQLType.from_pytype(int)
    assert isinstance(t, SQLType)
    assert t.sql_type == "integer"
    assert t == "integer"
    assert t == int
    assert t != bool
    assert t == SQLType("integer")

    assert SQLType.from_pytype("varchar").sql_type == "varchar"
    assert SQLType.from_pytype(set).sql_type == "set"

    class T:
        __sqltype__ = "customtype"
        
    assert SQLType.from_pytype(T).sql_type == "customtype"


def test_encrypted():
    key = hashlib.md5(b"key").digest()
    t = Encrypted(key)
    assert t.sql_type == "text"
    assert t.dumper("value") != "value"
    assert t.loader(t.dumper("value")) == "value"