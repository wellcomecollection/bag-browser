#!/usr/bin/env python

import collections
import contextlib
import datetime
import functools
import humanize
import json
import math
import os
import sqlite3

import attr
import boto3
from flask import Flask, Response, jsonify, render_template, request
from wellcome_storage_service import StorageServiceClient
from zipstreamer import ZipFile, ZipStream


app = Flask(__name__)

app.jinja_env.filters["naturaltime"] = humanize.naturaltime


@app.template_filter("to_json")
def to_json(s):
    return json.dumps(s)


@attr.s
class QueryContext:
    space = attr.ib()
    external_identifier_prefix = attr.ib()
    page = attr.ib()
    created_after = attr.ib()
    created_before = attr.ib()
    page_size = attr.ib(default=250)

    def __attrs_post_init__(self):
        if (
            self.created_before
            and self.created_after
            and self.created_after > self.created_before
        ):
            raise ValueError(
                f"created_before {self.created_before!r} is after created_after {self.created_after!r}!"
            )


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


def query_bags_db(query_context: QueryContext):
    with get_cursor("bags.db") as cursor:
        cursor.execute(
            """SELECT SUM(file_count), SUM(file_size), COUNT(*)
            FROM bags
            WHERE space=?
            AND external_identifier > ? AND external_identifier <= ? || 'z'
            AND date_created >= ? AND date_created <= ? || 'z'""",
            (
                query_context.space,
                query_context.external_identifier_prefix,
                query_context.external_identifier_prefix,
                query_context.created_after or "2000-01-01",
                query_context.created_before or "3000-01-01",
            ),
        )

        (total_file_count, total_file_size, total,) = cursor.fetchone()

        cursor.execute(
            """SELECT extension, SUM(count)
            FROM file_types
            WHERE bag_id in (
                SELECT id
                FROM bags
                WHERE space=?
                AND external_identifier > ? AND external_identifier <= ? || 'z'
                AND date_created >= ? AND date_created <= ? || 'z'
            )
            GROUP BY extension""",
            (
                query_context.space,
                query_context.external_identifier_prefix,
                query_context.external_identifier_prefix,
                query_context.created_after or "2000-01-01",
                query_context.created_before or "3000-01-01",
            ),
        )

        file_type_tally = dict(cursor.fetchall())

        cursor.execute(
            """SELECT *
            FROM bags
            WHERE space=?
            AND external_identifier > ? AND external_identifier <= ? || 'z'
            AND date_created >= ? AND date_created <= ? || 'z'
            ORDER BY id
            LIMIT ?,?""",
            (
                query_context.space,
                query_context.external_identifier_prefix,
                query_context.external_identifier_prefix,
                query_context.created_after or "2000-01-01",
                query_context.created_before or "3000-01-01",
                (query_context.page - 1) * query_context.page_size,
                query_context.page_size,
            ),
        )

        fields = [desc[0] for desc in cursor.description]

        matching_bags = [dict(zip(fields, bag)) for bag in cursor.fetchall()]

        for b in matching_bags:
            b["date_created_pretty"] = render_date(b["date_created"])
            b["file_count_pretty"] = humanize.intcomma(b["file_count"])
            b["file_size_pretty"] = humanize.naturalsize(b["file_size"])

        return {
            "total": total,
            "total_file_size": total_file_size,
            "total_file_count": total_file_count,
            "file_type_tally": file_type_tally,
            "bags": matching_bags,
        }


@app.route("/spaces/<space>/get_bags_data")
def get_bags_data(space):
    query_context = QueryContext(
        space=space,
        external_identifier_prefix=request.args.get("prefix", ""),
        page=int(request.args.get("page", "1")),
        created_after=request.args.get("created_after"),
        created_before=request.args.get("created_before"),
    )

    result = query_bags_db(query_context)

    return jsonify({
        "bags": result["bags"],
        "total_bags": humanize.intcomma(result["total"]),
        "total_file_count": humanize.intcomma(result["total_file_count"]),
        "total_file_size": humanize.naturalsize(result["total_file_size"]),
        "file_stats": result["file_type_tally"],
    })


@app.route("/bags/<space>/get_file_types")
def get_file_types(space):
    query_context = QueryContext(
        space=space,
        external_identifier_prefix=request.args.get("prefix", ""),
        page=int(request.args.get("page", "1")),
        created_after=request.args.get("created_after"),
        created_before=request.args.get("created_before"),
    )

    result = query_bags_db(query_context)

    print(result)
    return "no"


@app.route("/spaces/<space>")
def list_bags_in_space(space):
    query_context = QueryContext(
        space=space,
        external_identifier_prefix=request.args.get("prefix", ""),
        page=int(request.args.get("page", "1")),
        created_after=request.args.get("created_after", ""),
        created_before=request.args.get("created_before", ""),
    )

    result = query_bags_db(query_context)
    total_pages = int(math.ceil(result["total"] / query_context.page_size))

    return render_template(
        "bags_in_space.html",
        space=space,
        page=query_context.page,
        total_pages=total_pages,
        query_context=query_context,
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
