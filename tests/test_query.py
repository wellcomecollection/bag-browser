import pytest

from src.query import QueryContext


def test_can_query_correctly_ordered_created_date():
    QueryContext(
        space="digitised",
        external_identifier_prefix="b1",
        created_after="2001-01-01",
        created_before="2002-02-02",
        page=1,
    )


def test_can_query_single_created_date():
    QueryContext(
        space="digitised",
        external_identifier_prefix="b1",
        created_after="2001-01-01",
        created_before="2001-01-01",
        page=1,
    )


def test_invalid_created_date_is_error():
    with pytest.raises(ValueError, match="is after created_after"):
        QueryContext(
            space="digitised",
            external_identifier_prefix="b1",
            created_after="2002-01-01",
            created_before="2001-01-01",
            page=1,
        )
