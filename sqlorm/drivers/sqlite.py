import sqlite3
from sqlite3 import *


def connect(*args, **kwargs):
    conn = sqlite3.connect(*args, **kwargs)
    conn.row_factory = Row
    return conn