import json

from models import BagIdentifier, Bag


def test_bag_id():
    bag = Bag(
        identifier=BagIdentifier(
            space="example", external_identifier="1234", version=1
        ),
        created_date="2020-01-01T01:01:01.000000Z",
        file_count=1,
        total_file_size=1,
        file_ext_tally={".xml": 1},
    )

    assert bag.id == "example/1234/v1"


def test_can_serialise_from_storage_manifest():
    storage_manifest = json.load(open("tests/manifests/b10109377.json"))

    bag = Bag.from_storage_manifest(storage_manifest)

    assert bag.space == "digitised"
    assert bag.external_identifier == "b10109377"
    assert bag.version == 1
    assert bag.display_version == "v1"
    assert bag.created_date == "2019-12-16T10:01:44.021334Z"
    assert bag.file_count == 17
    assert bag.total_file_size == 18963115
    assert bag.file_ext_tally == {".jp2": 10, ".xml": 7}


def test_file_ext_tally_is_lowercased():
    bag = Bag(
        identifier=BagIdentifier(
            space="example", external_identifier="1234", version=1
        ),
        created_date="2020-01-01T01:01:01.000000Z",
        file_count=4,
        total_file_size=4,
        file_ext_tally={".xml": 1, ".XML": 1, ".jpg": 1, ".JP2": 1},
    )

    assert bag.file_ext_tally == {".xml": 2, ".jpg": 1, ".jp2": 1}
