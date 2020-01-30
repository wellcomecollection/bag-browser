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


PAGE_SIZE = 500


def get_bags_by_filter(space, external_identifier_prefix):
    page = int(request.args.get("page", "1"))

    with get_cursor("bags.db") as cursor:
        cursor.execute("""SELECT COUNT(*)
            FROM bags
            WHERE space=?
            AND external_identifier > ? AND external_identifier <= ? || 'z'""", (space, external_identifier_prefix, external_identifier_prefix)
        )

        total, = cursor.fetchone()

        cursor.execute("""SELECT *
            FROM bags
            WHERE space=?
            AND external_identifier > ? AND external_identifier <= ? || 'z'
            ORDER BY id
            LIMIT ?,?""", (space, external_identifier_prefix, external_identifier_prefix, (page - 1) * PAGE_SIZE, PAGE_SIZE)
        )

        fields = [desc[0] for desc in cursor.description]

        matching_bags = [dict(zip(fields, bag)) for bag in cursor.fetchall()]

        for b in matching_bags:
            b["date_created_pretty"] = render_date(b["date_created"])
            b["file_count"] = humanize.intcomma(b["file_count"])
            b["file_size"] = humanize.naturalsize(b["file_size"])

        return {
            "total": total,
            "bags": matching_bags,
            "page": page,
        }


@app.route("/spaces/<space>/search/<prefix>")
def list_bags_in_space_matching_prefix(space, prefix):
    result = get_bags_by_filter(
        space=space, external_identifier_prefix=prefix
    )

    for b in result["bags"]:
        del b["file_stats"]

    return jsonify(result["bags"])


@app.route("/spaces/<space>")
def list_bags_in_space(space):
    external_identifier_prefix = request.args.get("prefix", "")

    result = get_bags_by_filter(space=space, external_identifier_prefix=external_identifier_prefix)
    total_pages = int(math.ceil(result["total"] / PAGE_SIZE))

    bags_in_space = result["bags"]

    for b in bags_in_space:
        print(b)
        del b["file_stats"]


    bags_json = json.dumps(bags_in_space)

    return render_template(
        "bags_in_space.html", bags_in_space=bags_in_space, bags_json=bags_json, space=space, page=result["page"], total_pages=total_pages, external_identifier_prefix=external_identifier_prefix
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
