#!/usr/bin/env python

import itertools
import sqlite3

from src.database import SqliteDatabase
from src.storage_service import StorageService

import tqdm


def create_table(cursor):
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
        else:
            raise


def chunked_iterable(iterable, size):
    # https://alexwlchan.net/2018/12/iterating-in-fixed-size-chunks/
    it = iter(iterable)
    while True:
        chunk = tuple(itertools.islice(it, size))
        if not chunk:
            break
        yield chunk


if __name__ == "__main__":
    db = SqliteDatabase("bags_new.db")

    ss = StorageService(table_name="vhs-storage-manifests")

    with db.conn_cursor() as (conn, cursor):
        create_table(cursor)
        conn.commit()

        # What bags do we already have?
        cursor.execute("SELECT id FROM bags")
        known_bag_ids = {result[0] for result in cursor.fetchall()}

        def all_bags():
            total_bags = ss.total_bags()

            for bag_identifier in tqdm.tqdm(ss.get_bag_identifiers(), total=total_bags):
                if bag_identifier.id in known_bag_ids:
                    continue

                bag = ss.get_bag(bag_identifier)

                extension_counts = [
                    (bag.id, extension, count)
                    for extension, count in bag.file_ext_tally.items()
                ]
                cursor.executemany(
                    "INSERT INTO file_extensions(bag_id, extension, count) VALUES (?,?,?)",
                    extension_counts
                )

                yield bag

        for chunk in chunked_iterable(all_bags(), size=100):
            values = [
                (bag.id, bag.space, bag.external_identifier, bag.version, bag.created_date, bag.file_count, bag.total_file_size)
                for bag in chunk
            ]

            cursor.executemany("INSERT INTO bags(id, space, external_identifier, version, created_date, file_count, total_file_size) VALUES (?,?,?,?,?,?,?)", values)
            conn.commit()
