import psycopg2
from psycopg2 import pool
from contextlib import contextmanager


class DBPool:
    _instance = None

    def __init__(self, min_conn=2, max_conn=10):
        if not DBPool._instance:
            DBPool._instance = psycopg2.pool.ThreadedConnectionPool(
                min_conn, max_conn,
                user="your_user",
                password="your_password",
                host="your_host",
                port="5432",
                database="stock_db"
            )

    @classmethod
    def get_connection(cls):
        if not cls._instance:
            cls(min_conn=2, max_conn=10)
        return cls._instance.getconn()

    @classmethod
    def return_connection(cls, conn):
        cls._instance.putconn(conn)


@contextmanager
def get_db_connection():
    conn = DBPool.get_connection()
    try:
        yield conn
    finally:
        DBPool.return_connection(conn)