#!/usr/bin/env python

import contextlib
import functools
import itertools
import json
import os
import sqlite3

import boto3
import tqdm
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


def get_new_manifests(path):
    db_uri = "file://" + os.path.abspath(path) + "?mode=ro"

    with get_cursor(db_uri, uri=True) as (_, cursor_readonly):
        for storage_manifest in tqdm.tqdm(get_storage_manifest_ids()):
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

        def all_manifests():
            for manifest in get_new_manifests("bags.db"):
                yield (
                    "/".join([manifest["space"], manifest["external_identifier"], f"v{manifest['version']}"]),
                    manifest["space"],
                    manifest["external_identifier"],
                    manifest["version"],
                    manifest["date_created"],
                    manifest["file_count"],
                    manifest["file_size"]
                )

        for chunk in chunked_iterable(all_manifests(), size=100):
            cursor.executemany("INSERT INTO bags VALUES (?,?,?,?,?,?,?)", chunk)
            conn.commit()