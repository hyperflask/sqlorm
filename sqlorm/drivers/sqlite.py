import sqlite3
from sqlite3 import *


# Fine tune sqlite for highly concurrent workloads like web servers
# See https://fractaledmind.github.io/2023/09/07/enhancing-rails-sqlite-fine-tuning/
FINE_TUNED_PRAGMAS = {
    "journal_mode": "WAL",
    "synchronous": "NORMAL",
    "journal_size_limit": "67108864", # 64mb
    "mmap_size": "134217728", # 128mb
    "cache_size": "2000",
    "busy_timeout": "5000"
}


def connect(*args, **kwargs):
    pragmas = kwargs.pop("pragma", {})
    extensions = kwargs.pop("ext", None)
    fine_tune = kwargs.pop("fine_tune", False)

    kwargs.setdefault("check_same_thread", False) # better default to work with sqlorm pooling
    conn = sqlite3.connect(*args, **kwargs)
    conn.row_factory = Row

    if fine_tune:
        pragmas.update(FINE_TUNED_PRAGMAS)

    if pragmas:
        for key, value in pragmas.items():
            conn.execute(f"PRAGMA {key} = {value}")

    if extensions:
        conn.enable_load_extension(True)
        for ext in extensions:
            conn.execute(f"SELECT load_extension('{ext}')")

    return conn

