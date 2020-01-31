import contextlib
import pathlib
import sqlite3
from typing import List

import attr

from src.models import Bag
from src.query import QueryContext, QueryResult


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


@attr.s
class BagsDatabase:
    """
    A wrapper around SqliteDatabase with operations for handling bags.
    """
    database = attr.ib()

    def __attrs_post_init__(self):
        self._create_tables()

    @database.validator
    def _check_database(self, attribute, value):
        if not isinstance(value, SqliteDatabase):
            raise TypeError("database must be a SqliteDatabase")

    def _create_tables(self):
        with self.database.cursor() as cursor:
            try:
                cursor.execute(
                    """CREATE TABLE bags
                    (
                        id TEXT PRIMARY KEY,
                        space TEXT,
                        external_identifier TEXT,
                        version INTEGER,
                        created_date TEXT,
                        file_count INTEGER,
                        total_file_size INTEGER
                    )"""
                )
            except sqlite3.OperationalError as err:
                if str(err) == "table bags already exists":
                    pass
                else:
                    raise

            try:
                cursor.execute(
                    """CREATE TABLE file_extensions (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        bag_id TEXT,
                        extension TEXT,
                        count INTEGER,
                        CONSTRAINT fk_storage_key
                            FOREIGN KEY (bag_id)
                            REFERENCES bags(id),
                        UNIQUE (bag_id, extension)
                    )"""
                )
            except sqlite3.OperationalError as err:
                if str(err) == "table file_extensions already exists":
                    pass

                # I can't think of a way to trigger an unexpected error here
                # that wouldn't have thrown when creating the bags table.
                else:  # pragma: no cover
                    raise

    @classmethod
    def from_path(cls, path):
        return cls(database=SqliteDatabase(path=path))

    def get_known_ids(self):
        with self.database.read_only_cursor() as cursor:
            cursor.execute("SELECT id FROM bags")
            return {result[0] for result in cursor.fetchall()}

    @contextlib.contextmanager
    def bulk_store_bags(self):
        """
        A helper for storing bags that reuses the cursor/connection.
        To use:

            with bags_db.bulk_store_bags() as bulk_helper:
                for bag in bags_to_store:
                    bulk_helper.store_bag(bag)

        """
        with self.database.conn_cursor() as (conn, cursor):

            class Helper:
                def store_bag(self, bag):
                    extension_counts = [
                        (bag.id, extension, count)
                        for extension, count in bag.file_ext_tally.items()
                    ]
                    cursor.executemany(
                        """INSERT INTO file_extensions(bag_id, extension, count)
                        VALUES (?,?,?)""",
                        extension_counts,
                    )

                    cursor.execute(
                        """INSERT INTO bags(id, space, external_identifier, version, created_date, file_count, total_file_size)
                        VALUES (?,?,?,?,?,?,?)""",
                        (
                            bag.id,
                            bag.space,
                            bag.external_identifier,
                            bag.version,
                            bag.created_date,
                            bag.file_count,
                            bag.total_file_size,
                        ),
                    )
                    conn.commit()

            yield Helper()

    def query(self, query_context: QueryContext) -> QueryResult:
        with self.database.read_only_cursor() as cursor:
            cursor.execute(
                """SELECT SUM(file_count), SUM(total_file_size), COUNT(*)
                FROM bags
                WHERE space=?
                AND external_identifier > ? AND external_identifier <= ? || 'z'
                AND created_date >= ? AND created_date <= ? || 'z'""",
                (
                    query_context.space,
                    query_context.external_identifier_prefix,
                    query_context.external_identifier_prefix,
                    query_context.created_after or "2000-01-01",
                    query_context.created_before or "3000-01-01",
                ),
            )

            (total_file_count, total_file_size, total_count,) = cursor.fetchone()

            cursor.execute(
                """SELECT extension, SUM(count)
                FROM file_extensions
                WHERE bag_id in (
                    SELECT id
                    FROM bags
                    WHERE space=?
                    AND external_identifier > ? AND external_identifier <= ? || 'z'
                    AND created_date >= ? AND created_date <= ? || 'z'
                )
                GROUP BY extension""",
                (
                    query_context.space,
                    query_context.external_identifier_prefix,
                    query_context.external_identifier_prefix,
                    query_context.created_after or "2000-01-01",
                    query_context.created_before or "3000-01-01",
                ),
            )

            file_type_tally = dict(cursor.fetchall())

            cursor.execute(
                """SELECT *
                FROM bags
                WHERE space=?
                AND external_identifier > ? AND external_identifier <= ? || 'z'
                AND created_date >= ? AND created_date <= ? || 'z'
                ORDER BY id
                LIMIT ?,?""",
                (
                    query_context.space,
                    query_context.external_identifier_prefix,
                    query_context.external_identifier_prefix,
                    query_context.created_after or "2000-01-01",
                    query_context.created_before or "3000-01-01",
                    (query_context.page - 1) * query_context.page_size,
                    query_context.page_size,
                ),
            )

            fields = [desc[0] for desc in cursor.description]

            matching_bags = [dict(zip(fields, bag)) for bag in cursor.fetchall()]

        return QueryResult(
            total_count=total_count,
            total_file_count=total_file_count,
            total_file_size=total_file_size,
            file_type_tally=file_type_tally,
            bags=matching_bags
        )

    def get_spaces(self):
        with self.database.read_only_cursor() as cursor:
            cursor.execute("SELECT space, COUNT(space) FROM bags GROUP BY space")

            return dict(cursor.fetchall())
