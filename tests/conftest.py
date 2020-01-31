import pytest

from src.database import SqliteDatabase


@pytest.fixture
def db(tmpdir):
    db_path = tmpdir / "bags.db"
    yield SqliteDatabase(path=db_path)
