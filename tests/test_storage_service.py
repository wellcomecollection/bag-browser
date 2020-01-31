import contextlib
import secrets

import boto3
from moto import mock_dynamodb2, mock_s3

from src.models import Bag, BagIdentifier
from src.storage_service import StorageService


@contextlib.contextmanager
def manifests_table():
    table_name = f"table-{secrets.token_hex()}"

    dynamodb = boto3.client("dynamodb")

    dynamodb.create_table(
        AttributeDefinitions=[
            {"AttributeName": "id", "AttributeType": "S"},
            {"AttributeName": "version", "AttributeType": "N"},
        ],
        TableName=table_name,
        KeySchema=[
            {"AttributeName": "id", "KeyType": "HASH"},
            {"AttributeName": "version", "KeyType": "RANGE"},
        ],
    )

    yield table_name

    dynamodb.delete_table(TableName=table_name)


@contextlib.contextmanager
def s3_bucket():
    s3 = boto3.client("s3")

    bucket_name = f"s3-{secrets.token_hex(5)}"
    s3.create_bucket(Bucket=bucket_name)
    yield bucket_name

    # We can't delete a bucket unless it's empty, so we have to delete all
    # the objects first.
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket_name):
        for s3_obj in page["Contents"]:
            s3.delete_object(Bucket=bucket_name, Key=s3_obj["Key"])

    s3.delete_bucket(Bucket=bucket_name)


@mock_dynamodb2
def test_can_read_single_bag_id():
    dynamodb = boto3.resource("dynamodb")

    with manifests_table() as table_name:
        table = dynamodb.Table(table_name)
        table.put_item(Item={"id": "example_space/example_identifier", "version": 2})

        ss = StorageService(table_name=table_name)
        result = list(ss.get_bag_identifiers())

    assert result == [
        BagIdentifier(
            space="example_space", external_identifier="example_identifier", version=2
        )
    ]


@mock_dynamodb2
def test_can_read_lots_of_bag_ids():
    dynamodb = boto3.resource("dynamodb")

    with manifests_table() as table_name:
        table = dynamodb.Table(table_name)

        for version in range(1250):
            table.put_item(
                Item={"id": "example_space/example_identifier", "version": version}
            )

        ss = StorageService(table_name=table_name)
        assert len(list(ss.get_bag_identifiers())) == 1250


@mock_dynamodb2
@mock_s3
def test_can_get_bag():
    dynamodb = boto3.resource("dynamodb")
    s3 = boto3.client("s3")

    with manifests_table() as table_name, s3_bucket() as bucket_name:
        table = dynamodb.Table(table_name)

        s3.upload_file(
            Bucket=bucket_name,
            Key="b10109377.json",
            Filename="tests/manifests/b10109377.json",
        )

        table.put_item(
            Item={
                "id": "digitised/b10109377",
                "version": 1,
                "payload": {
                    "typedStoreId": {"namespace": bucket_name, "path": "b10109377.json"}
                },
            }
        )

        ss = StorageService(table_name=table_name)
        bag_identifier = next(ss.get_bag_identifiers())

        bag = ss.get_bag(bag_identifier=bag_identifier)

        assert isinstance(bag, Bag)
        assert bag.id == "digitised/b10109377/v1"


@mock_dynamodb2
def test_can_count_bags():
    dynamodb = boto3.resource("dynamodb")

    with manifests_table() as table_name:
        table = dynamodb.Table(table_name)

        table.put_item(
            Item={
                "id": "digitised/b10109377",
                "version": 1,
                "payload": {
                    "typedStoreId": {
                        "namespace": "s3-example",
                        "path": "b10109377.json",
                    }
                },
            }
        )

        ss = StorageService(table_name=table_name)
        assert ss.total_bags() == 1
