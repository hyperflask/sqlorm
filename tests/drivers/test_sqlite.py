from sqlorm import Engine


def test_sqlite_fine_tuning():
    e = Engine.from_uri("sqlite://:memory:?fine_tune=True")
    with e as tx:
        assert tx.fetchscalar("PRAGMA busy_timeout") == 5000