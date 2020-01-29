#!/usr/bin/env python

import json
import os

import boto3
from flask import Flask, Response, jsonify, render_template, request
from wellcome_storage_service import StorageServiceClient
from zipstreamer import ZipStream, ZipFile


s3 = boto3.client("s3")

app = Flask(__name__)


def get_storage_client(api_url):
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


storage_client = get_storage_client("https://api.wellcomecollection.org/storage/v1")


@app.route("/")
def index():
    start_after = request.args.get("start_after", "")

    resp = s3.list_objects_v2(
        Bucket="wellcomecollection-vhs-storage-manifests",
        StartAfter=start_after
    )

    entries = []
    for s3_obj in resp["Contents"]:
        key = os.path.dirname(s3_obj["Key"])
        space, name = key.split("/", 1)
        external_identifier, version = name.rsplit("/", 1)
        entries.append(
            {
                "space": space,
                "external_identifier": external_identifier,
                "version": f"v{version}",
            }
        )

    next_page = os.path.dirname(resp["Contents"][-1]["Key"])

    return render_template("entries.html", entries=entries, next_page = next_page)


@app.route("/bag/<space>/<external_identifier>/<version>")
def show_bag(space, external_identifier, version):
    return jsonify(
        storage_client.get_bag(
            space_id=space, source_id=external_identifier, version=version
        )
    )


@app.route("/download/<space>/<external_identifier>/<version>")
def download_bag(space, external_identifier, version):
    bag = storage_client.get_bag(
        space_id=space, source_id=external_identifier, version=version
    )

    def files():
        for bag_file in bag["manifest"]["files"] + bag["tagManifest"]["files"]:
            bucket = bag["location"]["bucket"]
            key = os.path.join(bag["location"]["path"], bag_file["path"])

            def create_fp(bucket, key):
                def inner():
                    return s3.get_object(Bucket=bucket, Key=key)["Body"]

                return inner

            yield ZipFile(
                filename=bag_file["name"],
                size=bag_file["size"],
                create_fp=create_fp(bucket, key),
                datetime=None,
                comment=None,
            )

    zs = ZipStream(files=list(files()))

    resp = Response(zs.generate(), mimetype="application/zip")
    resp.headers["Content-Disposition"] = "attachment; filename=bag.zip"
    resp.headers["Content-Length"] = str(zs.size())

    return resp


if __name__ == "__main__":
    app.run(debug=True)
