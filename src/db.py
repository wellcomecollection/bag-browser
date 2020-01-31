import contextlib
import pathlib
import sqlite3

import attr


@attr.s
class Database:
    path = attr.ib(converter=pathlib.Path)

    @contextlib.contextmanager
    def with_cursor(self, path):
        conn = sqlite3.connect(self.path)
        yield conn, conn.cursor()
        conn.commit()
        conn.close()

    @contextlib.contextmanager
    def with_read_only_cursor(self, path):
        conn = sqlite3.connect(f"file://{self.path.resolve()}", uri=True)
        yield conn.cursor()
        conn.commit()
        conn.close()
