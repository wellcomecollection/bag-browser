#!/usr/bin/env python

from src.database import BagsDatabase
from src.storage_service import StorageService

import tqdm


if __name__ == "__main__":
    bags_database = BagsDatabase.from_path("bags.db")

    known_bag_ids = bags_database.get_known_ids()

    ss = StorageService(table_name="vhs-storage-manifests")
    total_bags = ss.total_bags()

    for bag_identifier in tqdm.tqdm(ss.get_bag_identifiers(), total=total_bags):
        if bag_identifier.id in known_bag_ids:
            continue

        bag = ss.get_bag(bag_identifier)

        with bags_database.bulk_store_bags() as bulk_helper:
            bulk_helper.store_bag(bag)
