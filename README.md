## If you want to work on the app

Install [tox](https://pypi.org/project/tox/):

```console
$ pip3 install --user tox
```

Then run the following two commands to run linting and tests:

```console
$ tox -e lint
$ tox -e py37
```

## Known issues

*   Quite slow – we fetch every query twice, and we shouldn't do that.
    There's some profiling of the queries -- looks like assembling the file tally is a bit slow.
*   Version ordering is dodgy, see http://127.0.0.1:5000/spaces/digitised?prefix=b3136675
*   The JavaScript is a bit fragile
