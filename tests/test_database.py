import sqlite3

import pytest

from src.database import SqliteDatabase


@pytest.fixture
def db(tmpdir):
    db_path = tmpdir / "bags.db"
    yield SqliteDatabase(path=db_path)


def test_can_connect_to_db(tmpdir):
    db_path = tmpdir / "bags.db"
    db = SqliteDatabase(path=db_path)

    with db.cursor() as cursor:
        cursor.execute("CREATE TABLE words (word TEXT PRIMARY KEY)")
        cursor.execute("INSERT INTO WORDS(word) VALUES (?)", ("hello",))

    assert db_path.exists()


def test_can_commit_midway(db):
    with db.conn_cursor() as (conn, cursor):
        cursor.execute("CREATE TABLE words (word TEXT PRIMARY KEY)")
        cursor.execute("INSERT INTO WORDS(word) VALUES (?)", ("hello",))

        with db.read_only_cursor() as ro_cursor:
            ro_cursor.execute("SELECT COUNT(*) FROM words")
            assert ro_cursor.fetchone() == (0,)

        conn.commit()

        with db.read_only_cursor() as ro_cursor:
            ro_cursor.execute("SELECT COUNT(*) FROM words")
            assert ro_cursor.fetchone() == (1,)


def test_read_only_cursor_is_readonly(db):
    with db.cursor() as cursor:
        cursor.execute("CREATE TABLE words (word TEXT PRIMARY KEY)")

    with db.read_only_cursor() as cursor:
        with pytest.raises(
            sqlite3.OperationalError, match="attempt to write a readonly database"
        ):
            cursor.execute("CREATE TABLE names (word TEXT PRIMARY KEY)")
