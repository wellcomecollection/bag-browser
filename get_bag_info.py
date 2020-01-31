#!/usr/bin/env python

from src.database import BagsDatabase
from src.storage_service import StorageService

import tqdm


if __name__ == "__main__":
    bags_database = BagsDatabase.from_path("bags_new2.db")

    known_bag_ids = bags_database.get_known_ids()

    # ss = StorageService(table_name="vhs-storage-manifests")
    # total_bags = ss.total_bags()

    # db = SqliteDatabase("bags_new.db")

    def get_all_bags():
        import os, json
        from src.models import Bag
        for dirpath, _, filenames in os.walk("manifests"):
            if len(filenames) != 1:
                continue

            storage_manifest = json.load(open(os.path.join(dirpath, filenames[0])))

            yield Bag.from_storage_manifest(storage_manifest)

    for bag in tqdm.tqdm(get_all_bags()):
        if bag.id in known_bag_ids:
            continue

        with bags_database.bulk_store_bags() as bulk_helper:
            bulk_helper.store_bag(bag)

        # print(bag.id)
        # break

    # for bag_identifier in tqdm.tqdm(ss.get_bag_identifiers(), total=total_bags):
    #     if bag_identifier.id in known_bag_ids:
    #         continue
    #
    #     bag = ss.get_bag(bag_identifier)
    #

