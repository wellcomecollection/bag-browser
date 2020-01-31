# Implementation notes

## Database

The app uses a SQLite database to keep a local cache of the bags data.

The storage service APIs don't expose quite the data we need, so we fetch it by scraping the AWS account:

*   Scan the DynamoDB table `vhs-storage-manifests`, which records every registered bag
*   For every bag not in the SQLite database, fetch the storage manifest from S3, and record the bag information in SQLite

Because bags never change, we can run this process repeatedly to "top up" the local database -- we don't need to worry that a previously-stored bag might need to be updated.

The querying is also done in SQL.

Interesting files:

*   [`src/models.py`](../src/models.py) for the Bag model, which holds all the information we know about a bag
*   [`src/database.py`](../src/database.py) for the table schemas, and methods that store/retrieve bags from the database



## Web app

The web app is a [Flask app](https://palletsprojects.com/p/flask/).

When you're browsing the bags in the space, there's a form where the user can filter the results.
There's an event handler for the `onchange`/`oninput` events on the `<input>` elements in that form.
When the handler fires:

*   The appropriate method on the JavaScript class `QueryContext` is called.

    For example, if the user changed the external identifier prefix, the method `QueryContext.changeExternalIdentifierPrefix` is called.

*   This triggers a request to the Python app, asking for an updated set of results for the current query (see `QueryContext.updateResults`).

*   The Python app makes a new query, and returns the results to the browser.

*   When the results are received, the `QueryContext` instance calls `BagHandler.renderTable`, which recreates the table with the new results.

Interesting files:

*   [`templates/query_form.html`](../templates/query_form.html) for the form where the user can select their filters, and where the event handlers get bound
*   [`static/bag_browser.js`](../static/bag_browser.js) for the JavaScript classes
