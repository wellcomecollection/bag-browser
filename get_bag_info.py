#!/usr/bin/env python

import collections
import contextlib
import functools
import itertools
import json
import os
import sqlite3

import boto3
import tqdm
from wellcome_storage_service import BagNotFound, StorageServiceClient, ServerError as StorageServiceError


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


def get_storage_manifest_ids():
    dynamodb = boto3.resource("dynamodb").meta.client

    paginator = dynamodb.get_paginator("scan")

    for page in paginator.paginate(TableName="vhs-storage-manifests"):
        for item in page["Items"]:
            # The ID is of the form
            #
            #   {space}/{external_identifier}
            #
            space, external_identifier = item["id"].split("/")
            version = int(item["version"])

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
                id TEXT PRIMARY KEY,
                space TEXT,
                external_identifier TEXT,
                version INTEGER,
                date_created TEXT,
                file_count INTEGER,
                file_size INTEGER
            )"""
        )

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='file_types'")

    if cursor.fetchone() is None:
        cursor.execute(
            """CREATE TABLE file_types (
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


def get_new_manifests(known_bag_ids):
    for storage_manifest in tqdm.tqdm(get_storage_manifest_ids()):
        bag_id = "/".join([storage_manifest["space"], storage_manifest["external_identifier"], f"v{storage_manifest['version']}"])

        if bag_id in known_bag_ids:
            continue

        enrich_with_bag_info(storage_manifest)
        yield storage_manifest


@functools.lru_cache()
def get_bag(space, external_identifier, version):
    client = get_storage_client()

    try:
        return client.get_bag(
            space_id=space,
            source_id=external_identifier,
            version=f"v{version}",
        )
    except StorageServiceError:
        # TODO: The bags API has issues serving excessively large bags, so
        # we go behind its back to retrieve this bag.
        # This code should be removed when we fixed the bags API.
        # See https://github.com/wellcometrust/platform/issues/4024
        dynamodb = boto3.resource("dynamodb")

        resp = dynamodb.Table("vhs-storage-manifests").get_item(Key={
            "id": f"{space}/{external_identifier}",
            "version": version
        })

        item = resp["Item"]

        bucket = item["payload"]["typedStoreId"]["namespace"]
        key = item["payload"]["typedStoreId"]["path"]

        s3 = boto3.client("s3")
        body = s3.get_object(Bucket=bucket, Key=key)["Body"]
        manifest = json.load(body)

        return manifest


def enrich_with_bag_info(storage_manifest):
    client = get_storage_client()

    bag = get_bag(
        space=storage_manifest["space"],
        external_identifier=storage_manifest["external_identifier"],
        version=storage_manifest['version'],
    )

    if not bag:
        return

    count = len(bag["manifest"]["files"])
    size = sum(f["size"] for f in bag["manifest"]["files"])

    file_stats = dict(collections.Counter(
        os.path.splitext(f["name"])[1].lower()
        for f in bag["manifest"]["files"]
    ))

    storage_manifest["date_created"] = bag["createdDate"]
    storage_manifest["file_count"] = count
    storage_manifest["file_size"] = size
    storage_manifest["file_stats"] = file_stats


def chunked_iterable(iterable, size):
    # https://alexwlchan.net/2018/12/iterating-in-fixed-size-chunks/
    it = iter(iterable)
    while True:
        chunk = tuple(itertools.islice(it, size))
        if not chunk:
            break
        yield chunk


if __name__ == "__main__":
    with get_cursor("bags.db") as (conn, cursor):
        create_table(cursor)
        conn.commit()

        # What bags do we already have?
        cursor.execute("SELECT id FROM bags")
        known_bag_ids = {result[0] for result in cursor.fetchall()}

        def all_manifests():
            for manifest in get_new_manifests(known_bag_ids):
                manifest_id = "/".join([manifest["space"], manifest["external_identifier"], f"v{manifest['version']}"])
                yield (
                    manifest_id,
                    manifest["space"],
                    manifest["external_identifier"],
                    manifest["version"],
                    manifest["date_created"],
                    manifest["file_count"],
                    manifest["file_size"]
                )

                file_types = [
                    (manifest_id, extension, count)
                    for extension, count in manifest["file_stats"].items()
                ]
                cursor.executemany(
                    """INSERT INTO file_types(bag_id, extension, count) VALUES (?,?,?)""",
                    file_types
                )

        for chunk in chunked_iterable(all_manifests(), size=100):
            cursor.executemany("INSERT INTO bags VALUES (?,?,?,?,?,?,?)", chunk)
            conn.commit()
