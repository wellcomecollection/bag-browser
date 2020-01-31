import contextlib
import pathlib
import sqlite3

import attr


@attr.s
class SqliteDatabase:
    """
    A thin wrapper around a sqlite database that provides a connection, cursor
    and read-only cursor.
    """

    path = attr.ib(converter=pathlib.Path)

    @contextlib.contextmanager
    def conn_cursor(self):
        conn = sqlite3.connect(self.path)
        yield conn, conn.cursor()
        conn.commit()
        conn.close()

    @contextlib.contextmanager
    def cursor(self):
        with self.conn_cursor() as (_, cursor):
            yield cursor

    @contextlib.contextmanager
    def read_only_cursor(self):
        conn = sqlite3.connect(f"file://{self.path.resolve()}?mode=ro", uri=True)
        yield conn.cursor()
        conn.commit()
        conn.close()
