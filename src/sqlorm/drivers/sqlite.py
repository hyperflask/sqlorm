import sqlite3
from sqlite3 import *  # noqa


# Fine tune sqlite for highly concurrent workloads like web servers
# See https://fractaledmind.github.io/2023/09/07/enhancing-rails-sqlite-fine-tuning/
FINE_TUNED_PRAGMAS = {
    "journal_mode": "WAL",
    "synchronous": "NORMAL",
    "journal_size_limit": "67108864",  # 64mb
    "mmap_size": "134217728",  # 128mb
    "cache_size": "2000",
    "busy_timeout": "5000",
}


PRIMARY_KEY_SCHEMA_DEFINITION = "PRIMARY KEY AUTOINCREMENT"


def connect(filename, *args, **kwargs):
    pragmas = kwargs.pop("pragma", {})
    extensions = kwargs.pop("ext", None)
    fine_tune = kwargs.pop("fine_tune", False)
    foreign_keys = kwargs.pop("foreign_keys", False)
    ensure_path = kwargs.pop("ensure_path", False)

    if ensure_path and filename != ":memory:":
        import os
        os.makedirs(os.path.dirname(filename), exist_ok=True)

    kwargs.setdefault("check_same_thread", False)  # better default to work with sqlorm pooling
    conn = sqlite3.connect(filename, *args, **kwargs)
    conn.row_factory = sqlite3.Row

    if foreign_keys:
        pragmas["foreign_keys"] = "ON"
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
