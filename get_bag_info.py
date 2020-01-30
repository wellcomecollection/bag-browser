#!/usr/bin/env python

import contextlib
import functools
import json
import os
import sqlite3

import boto3
from wellcome_storage_service import BagNotFound, StorageServiceClient


@contextlib.contextmanager
def get_cursor(path, **kwargs):
    conn = sqlite3.connect(path, **kwargs)
    yield conn, conn.cursor()
    conn.commit()
    conn.close()


def get_storage_client(api_url="https://api.wellcomecollection.org/storage/v1"):
    creds_path = os.path.join(
        os.environ["HOME"], ".wellcome-storage", "oauth-credentials.json"
    )
    oauth_creds = json.load(open(creds_path))

    return StorageServiceClient(
        api_url=api_url,
        client_id=oauth_creds["client_id"],
        client_secret=oauth_creds["client_secret"],
        token_url=oauth_creds["token_url"],
    )


def get_storage_manifest_ids(bucket="wellcomecollection-vhs-storage-manifests"):
    s3 = boto3.client("s3")

    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket):
        for s3_obj in page["Contents"]:
            s3_key = s3_obj["Key"]

            # The S3 key is of the form
            #
            #   {space}/{external_identifier}/{version}/{VHS filename}
            #
            storage_id = os.path.dirname(s3_key)

            space, _remaining = storage_id.split("/", 1)
            external_identifier, version = _remaining.rsplit("/", 1)

            yield {
                "space": space,
                "external_identifier": external_identifier,
                "version": version,
            }


def create_table(cursor):
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='bags'")

    if cursor.fetchone() is None:
        cursor.execute(
            """CREATE TABLE bags
            (
                space TEXT,
                external_identifier TEXT,
                version INTEGER,
                date_created TEXT,
                file_count INTEGER,
                file_size INTEGER
            )"""
        )


def get_new_manifests(path):
    db_uri = "file://" + os.path.abspath(path) + "?mode=ro"

    with get_cursor(db_uri, uri=True) as (_, cursor_readonly):
        for storage_manifest in get_storage_manifest_ids():
            # Is this storage manifest already in the table?
            cursor_readonly.execute(
                "SELECT * FROM bags WHERE space=? AND external_identifier=? AND version=?",
                (
                    storage_manifest["space"],
                    storage_manifest["external_identifier"],
                    storage_manifest["version"],
                ),
            )

            if cursor_readonly.fetchone() is not None:
                continue

            get_bag_info(storage_manifest)
            yield storage_manifest


def get_bag_info(storage_manifest):
    client = get_storage_client()

    try:
        bag = client.get_bag(
            space_id=storage_manifest["space"],
            source_id=storage_manifest["external_identifier"],
            version=f"v{storage_manifest['version']}",
        )
    except BagNotFound:
        return None

    count = len(bag["manifest"]["files"])
    size = sum(f["size"] for f in bag["manifest"]["files"])

    storage_manifest["date_created"] = bag["createdDate"]
    storage_manifest["file_count"] = count
    storage_manifest["file_size"] = size


if __name__ == "__main__":
    with get_cursor("bags.db") as (conn, cursor):
        create_table(cursor)
        conn.commit()

        def all_manifests():
            for manifest in get_new_manifests("bags.db"):
                yield (
                    manifest["space"],
                    manifest["external_identifier"],
                    manifest["version"],
                    manifest["date_created"],
                    manifest["file_count"],
                    manifest["file_size"]
                )
                break

        cursor.executemany("INSERT INTO bags VALUES (?,?,?,?,?,?)", all_manifests())
