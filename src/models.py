import collections
import os

import attr


def _normalise_file_tally(tally):
    if all(ext.islower() for ext in tally):
        return tally
    else:
        new_tally = collections.defaultdict(int)
        for ext, count in tally.items():
            new_tally[ext.lower()] += count
        return dict(new_tally)


@attr.s
class BagIdentifier:
    space = attr.ib()
    external_identifier = attr.ib()
    version = attr.ib()

    @property
    def id(self):
        return "/".join([self.space, self.external_identifier, self.display_version])

    @property
    def display_version(self):
        return f"v{self.version}"


@attr.s
class Bag:
    """
    Holds all the information about a bag that we need to cache for the
    bag browser.
    """

    identifier = attr.ib()
    created_date = attr.ib()
    file_count = attr.ib()
    total_file_size = attr.ib()
    file_ext_tally = attr.ib(converter=_normalise_file_tally)
    storage_manifest = attr.ib(default=None)

    @property
    def space(self):
        return self.identifier.space

    @property
    def external_identifier(self):
        return self.identifier.external_identifier

    @property
    def version(self):
        return self.identifier.version

    @property
    def id(self):
        return self.identifier.id

    @property
    def display_version(self):
        return self.identifier.display_version

    @classmethod
    def from_storage_manifest(cls, storage_manifest):
        """
        Given a raw storage manifest (as stored in S3), turn it into a Bag.
        """
        files = storage_manifest["manifest"]["files"]

        file_ext_tally = dict(
            collections.Counter(os.path.splitext(f["name"])[1] for f in files)
        )

        return cls(
            identifier=BagIdentifier(
                space=storage_manifest["space"],
                external_identifier=storage_manifest["info"]["externalIdentifier"],
                version=storage_manifest["version"],
            ),
            created_date=storage_manifest["createdDate"],
            file_count=len(files),
            total_file_size=sum(f["size"] for f in files),
            file_ext_tally=file_ext_tally,
            storage_manifest=storage_manifest,
        )
