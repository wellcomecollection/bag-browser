import pytest

from src.database import BagsDatabase
from src.models import Bag, BagIdentifier
from src.query import QueryContext, QueryResult


bag1 = Bag(
    identifier=BagIdentifier(space="digitised", external_identifier="b1234", version=1),
    created_date="2001-01-01T01:01:01.000000Z",
    file_count=11,
    total_file_size=1100,
    file_ext_tally={".xml": 5, ".jp2": 6},
)


bag2 = Bag(
    identifier=BagIdentifier(space="digitised", external_identifier="b1235", version=1),
    created_date="2002-01-01T01:01:01.000000Z",
    file_count=3,
    total_file_size=400,
    file_ext_tally={".xml": 2, ".jp2": 1},
)


bag3 = Bag(
    identifier=BagIdentifier(
        space="born-digital", external_identifier="LE/MON/1", version=1
    ),
    created_date="2002-01-01T01:01:01.000000Z",
    file_count=6,
    total_file_size=200,
    file_ext_tally={".xml": 3, ".jp2": 3},
)


@pytest.fixture
def bags_db(db):
    bags_db = BagsDatabase(db)

    with bags_db.bulk_store_bags() as bulk_helper:
        for bag in (bag1, bag2, bag3):
            bulk_helper.store_bag(bag)

    yield bags_db


def test_can_filter_by_space(bags_db):
    query_context = QueryContext(space="born-digital", external_identifier_prefix="")

    result = bags_db.query(query_context)

    assert isinstance(result, QueryResult)
    assert result.total_count == 1
    assert result.total_file_count == 6
    assert result.total_file_size == 200
    assert result.file_ext_tally == {".xml": 3, ".jp2": 3}

    assert len(result.bags) == 1
    assert result.bags[0].id == bag3.id


def test_can_get_spaces(bags_db):
    assert bags_db.get_spaces() == {"digitised": 2, "born-digital": 1}


def test_handles_empty_results(db):
    bags_db = BagsDatabase(db)

    query_context = QueryContext(space="any", external_identifier_prefix="")

    result = bags_db.query(query_context)

    assert result.total_count == 0
    assert result.total_file_count == 0
    assert result.total_file_size == 0
    assert result.file_ext_tally == {}
