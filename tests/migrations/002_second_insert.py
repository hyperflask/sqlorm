from sqlorm import get_current_session

with get_current_session() as tx:
    tx.execute("INSERT INTO second_table VALUES (1, 'foo@foo.com')")