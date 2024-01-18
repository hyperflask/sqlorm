import mysql.connector
from mysql.connector import *


class Connection(mysql.connector.connection.MySQLConnection):
    def cursor(self, *args, **kwargs):
        kwargs["dictionary"] = True
        return super().cursor(*args, **kwargs)
    

def connect(*args, **kwargs):
    return Connection(*args, **kwargs)