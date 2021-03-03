import psycopg2
from psycopg2 import pool, extras
from config.config import GlobalConfig

from contextlib import contextmanager
from threading import Semaphore
from psycopg2 import pool, extensions

CONFIG = GlobalConfig()


class ReallyThreadedConnectionPool(psycopg2.pool.ThreadedConnectionPool):
    def __init__(self, minconn, maxconn, *args, **kwargs):
        self._semaphore = Semaphore(maxconn)
        super().__init__(minconn, maxconn, *args, **kwargs)

    def getconn(self, key=None):
        self._semaphore.acquire()
        return super().getconn(key)

    def putconn(self, *args, **kwargs):
        super().putconn(*args, **kwargs)
        self._semaphore.release()


cnxpool = ReallyThreadedConnectionPool(5, 10, **CONFIG.postgres_dict)


@contextmanager
def get_cursor():
    try:
        con = cnxpool.getconn()
        cursor = con.cursor()
        yield cursor
    except psycopg2.Error as e:
        print(e)
    finally:
        cursor.close()
        cnxpool.putconn(con)


class PyPgsql(object):
    @staticmethod
    def get_all(sql):
        with get_cursor() as cursor:
            cursor.execute(sql)
            return cursor.fetchall()


class POSTGERS:
    def get_all_tables(self):
        SQL = """
            SELECT tablename FROM pg_tables where schemaname = 'public';
        """
        result = PyPgsql.get_all(SQL)
        print(result)
        return [r[0] for r in result]

    def get_table_structures(self, table_name: str):
        column_type_list = []
        SQL = """
            SELECT a.attnum,
                a.attname AS field,
                t.typname AS type,
                a.attlen AS length,
                a.atttypmod AS lengthvar,
                a.attnotnull AS notnull,
                b.description AS comment
            FROM pg_class c,
                pg_attribute a
                LEFT OUTER JOIN pg_description b ON a.attrelid=b.objoid AND a.attnum = b.objsubid,
                pg_type t
            WHERE c.relname = '{table_name}'
                and a.attnum > 0
                and a.attrelid = c.oid
                and a.atttypid = t.oid
            ORDER BY a.attnum;
        """.format(table_name=table_name)
        x = PyPgsql.get_all(SQL)
        print(table_name)
        if x is not None:
            for column in x:
                print(column)
                column_type_list.append(column[2])
        return list(set(column_type_list))
