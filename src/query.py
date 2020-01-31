import attr


@attr.s
class QueryContext:
    """
    Holds all the state about a bags query.

    This mirrors a JavaScript class that holds the same data.

    """
    space = attr.ib()
    external_identifier_prefix = attr.ib()
    created_after = attr.ib()
    created_before = attr.ib()
    page = attr.ib()
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
