import sqlite3

import pytest

from src.database import BagsDatabase, SqliteDatabase
from src.models import Bag, BagIdentifier


def test_creates_tables(db):
    BagsDatabase(db)

    with db.cursor() as cursor:
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        names = {res[0] for res in cursor.fetchall()}

        assert "bags" in names
        assert "file_extensions" in names


def test_table_creation_is_idempotent(db):
    BagsDatabase(db)

    with db.cursor() as cursor:
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        names1 = {res[0] for res in cursor.fetchall()}

    BagsDatabase(db)

    with db.cursor() as cursor:
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        names2 = {res[0] for res in cursor.fetchall()}

    assert names1 == names2


def test_unexpected_error_upon_table_creation_is_raised():
    db = SqliteDatabase("/dev/null")

    with pytest.raises(sqlite3.OperationalError, match="unable to open database file"):
        BagsDatabase(db)


def test_passing_path_to_constructor_is_typeerror():
    with pytest.raises(TypeError, match="database must be a SqliteDatabase"):
        BagsDatabase("bad_database.db")


def test_can_create_bags_database_from_path(tmpdir):
    path = tmpdir / "bags.db"

    bags_db = BagsDatabase.from_path(path)

    assert bags_db.database == SqliteDatabase(path)
    assert path.exists()


def test_can_store_bags_in_database(db):
    bags_db = BagsDatabase(db)

    bag1 = Bag(
        identifier=BagIdentifier(
            space="example", external_identifier="1234", version=1
        ),
        created_date="2020-01-01T01:01:01.000000Z",
        file_count=4,
        total_file_size=4,
        file_ext_tally={".xml": 1, ".XML": 1, ".jpg": 1, ".JP2": 1},
    )

    bag2 = Bag(
        identifier=BagIdentifier(
            space="example", external_identifier="1234", version=2
        ),
        created_date="2020-01-01T01:01:01.000000Z",
        file_count=4,
        total_file_size=4,
        file_ext_tally={".xml": 1, ".XML": 1, ".jpg": 1, ".JP2": 1},
    )

    with bags_db.bulk_store_bags() as bulk_helper:
        bulk_helper.store_bag(bag1)
        bulk_helper.store_bag(bag2)

    with db.cursor() as cursor:
        cursor.execute("SELECT id FROM bags")
        bag_ids = {res[0] for res in cursor.fetchall()}

    assert bag_ids == {"example/1234/v1", "example/1234/v2"}
    assert bags_db.get_known_ids() == bag_ids
