#!/usr/bin/env python

import datetime
import functools
import humanize
import json
import math
import os
import subprocess

import attr
import boto3
from flask import Flask, Response, jsonify, render_template, request
from wellcome_storage_service import StorageServiceClient
from zipstreamer import ZipFile, ZipStream

from src.database import BagsDatabase
from src.models import BagIdentifier
from src.query import QueryContext
from src.storage_service import StorageService


app = Flask(__name__)

app.jinja_env.filters["intcomma"] = humanize.intcomma


GIT_COMMIT = subprocess.check_output(["git", "rev-parse", "HEAD"]).strip().decode("ascii")


@app.context_processor
def inject_commit():
    return {"git_commit": GIT_COMMIT}


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


@app.route("/")
def index():
    bags_database = BagsDatabase.from_path("bags.db")
    spaces = bags_database.get_spaces()

    return render_template("index.html", spaces=spaces)


PAGE_SIZE = 250


bags_database = BagsDatabase.from_path("bags.db")


def query_bags_db(query_context: QueryContext):
    query_result = bags_database.query(query_context)

    bags = []

    for bag in query_result.bags:
        b = attr.asdict(bag)
        b["id"] = bag.id
        b["created_date_pretty"] = render_date(b["created_date"])
        b["file_count_pretty"] = humanize.intcomma(b["file_count"])
        b["file_size_pretty"] = humanize.naturalsize(b["total_file_size"])
        bags.append(b)

    return {
        "total": query_result.total_count,
        "total_file_size": query_result.total_file_size,
        "total_file_count": query_result.total_file_count,
        "file_ext_tally": query_result.file_ext_tally,
        "bags": bags,
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
        "file_ext_tally": result["file_ext_tally"],
    })


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
    bag_identifier = BagIdentifier(
        space=space, external_identifier=external_identifier, version=version
    )

    ss = StorageService(table_name="vhs-storage-manifests")
    bag = ss.get_bag(bag_identifier)

    s3 = boto3.client("s3")

    def files():
        for bag_file in bag.files():
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
    app.run(debug=True, port=7913)
