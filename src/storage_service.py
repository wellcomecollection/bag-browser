import json
from typing import Iterable

import attr
import boto3

from src.models import Bag, BagIdentifier


@attr.s
class StorageService:
    table_name = attr.ib()

    def total_bags(self) -> int:
        """
        Get an approximate count for the number of bags in the storage service.
        """
        dynamodb = boto3.client("dynamodb")
        resp = dynamodb.describe_table(TableName=self.table_name)
        return resp["Table"]["ItemCount"]

    def get_bag_identifiers(self) -> Iterable[BagIdentifier]:
        dynamodb = boto3.resource("dynamodb").meta.client

        paginator = dynamodb.get_paginator("scan")

        for page in paginator.paginate(TableName=self.table_name):
            for item in page["Items"]:

                # The ID is of the form
                #
                #   {space}/{external_identifier}
                #
                space, external_identifier = item["id"].split("/")
                version = int(item["version"])

                yield BagIdentifier(
                    space=space,
                    external_identifier=external_identifier,
                    version=version,
                )

    def get_bag(self, bag_identifier: BagIdentifier) -> Bag:
        dynamodb = boto3.resource("dynamodb").meta.client
        s3 = boto3.client("s3")

        ddb_key = {
            "id": "/".join([bag_identifier.space, bag_identifier.external_identifier]),
            "version": bag_identifier.version,
        }

        ddb_resp = dynamodb.get_item(TableName=self.table_name, Key=ddb_key)

        item = ddb_resp["Item"]

        s3_bucket = item["payload"]["typedStoreId"]["namespace"]
        s3_key = item["payload"]["typedStoreId"]["path"]

        s3_body = s3.get_object(Bucket=s3_bucket, Key=s3_key)["Body"]
        storage_manifest = json.load(s3_body)

        return Bag.from_storage_manifest(storage_manifest)
