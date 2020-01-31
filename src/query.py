import attr


@attr.s
class QueryContext:
    """
    Holds all the state about a bags query.

    This mirrors a JavaScript class that holds the same data.

    """
    space = attr.ib()
    external_identifier_prefix = attr.ib()
    created_after = attr.ib(default="")
    created_before = attr.ib(default="")
    page = attr.ib(default=1)
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


@attr.s
class QueryResult:
    total_count = attr.ib()
    total_file_count = attr.ib()
    total_file_size = attr.ib()
    file_ext_tally = attr.ib()
    bags = attr.ib()
