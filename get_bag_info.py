#!/usr/bin/env python

import itertools
import sqlite3

from src.database import BagsDatabase, SqliteDatabase
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
    bags_database = BagsDatabase.from_path("bags_new2.db")

    known_bag_ids = bags_database.get_known_ids()

    ss = StorageService(table_name="vhs-storage-manifests")
    total_bags = ss.total_bags()

    # db = SqliteDatabase("bags_new.db")

    for bag_identifier in tqdm.tqdm(ss.get_bag_identifiers(), total=total_bags):
        if bag_identifier.id in known_bag_ids:
            continue

        bag = ss.get_bag(bag_identifier)

        with bags_database.bulk_store_bags() as bulk_helper:
            bulk_helper.store_bag(bag)
