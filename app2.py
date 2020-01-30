#!/usr/bin/env python

import contextlib
import datetime
import functools
import humanize
import json
import math
import os
import sqlite3

import boto3
from flask import Flask, Response, jsonify, render_template, request
from wellcome_storage_service import StorageServiceClient
from zipstreamer import ZipFile, ZipStream


app = Flask(__name__)

app.jinja_env.filters["intcomma"] = humanize.intcomma
app.jinja_env.filters["naturalsize"] = humanize.naturalsize
app.jinja_env.filters["naturaltime"] = humanize.naturaltime


@functools.lru_cache()
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


@contextlib.contextmanager
def get_cursor(path):
    uri = "file://" + os.path.abspath(path) + "?mode=ro"
    conn = sqlite3.connect(path, uri=True)
    yield conn.cursor()
    conn.commit()
    conn.close()


@app.route("/")
def index():
    with get_cursor("bags.db") as cursor:
        cursor.execute("SELECT space, COUNT(space) FROM bags GROUP BY space")

        spaces = dict(cursor.fetchall())

        return render_template("index.html", spaces=spaces)


@app.route("/spaces/<space>")
def list_bags_in_space(space):
    page = int(request.args.get("page", "1"))
    page_size = 1000

    with get_cursor("bags.db") as cursor:
        cursor.execute("""SELECT COUNT(*) FROM bags WHERE space=?""", (space,))
        total, = cursor.fetchone()
        total_pages = int(math.ceil(total / page_size))

        cursor.execute("""SELECT *
            FROM bags
            WHERE space=?
            ORDER BY id
            LIMIT ?,?""", (space, (page - 1) * page_size, page_size)
        )

        fields = [desc[0] for desc in cursor.description]

        bags_in_space = [dict(zip(fields, bag)) for bag in cursor.fetchall()]

        return render_template(
            "bags_in_space.html", bags_in_space=bags_in_space, space=space, page=page, total_pages=total_pages
        )


@app.route("/bags/<space>/<external_identifier>/v<version>/metadata")
def get_bag_metadata(space, external_identifier, version):
    storage_client = get_storage_client()

    return jsonify(
        storage_client.get_bag(
            space_id=space, source_id=external_identifier, version=f"v{version}"
        )
    )


@app.route("/bags/<space>/<external_identifier>/v<version>/files")
def get_bag_files(space, external_identifier, version):
    storage_client = get_storage_client()

    bag = storage_client.get_bag(
        space_id=space, source_id=external_identifier, version=f"v{version}"
    )

    s3 = boto3.client("s3")

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


@app.template_filter("render_date")
def render_date(date_string):
    date_obj = datetime.datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%S.%fZ")

    if date_obj.date() == datetime.datetime.now().date():
        return humanize.naturaltime(date_obj)
    else:
        return date_obj.date().isoformat()


if __name__ == "__main__":
    app.run(debug=True)
