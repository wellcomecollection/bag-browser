# Known issues

*   It's quite slow – when you load a page, it makes the query twice (once to render the initial page, once for the JavaScript), and we shouldn't do that.

    There's some profiling of the queries (see `database.py`), which shows which of the three queries we make on each request is the slow one.

    This is because the pagination buttons are only rendered when the page initially renders -- ideally these would update based on the query.
    If the user selects a query that no longer needs pagination, those buttons should disappear.
