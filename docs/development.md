# Developing the app

## Running the tests

If you want to make changes to the code, there are two tox tasks for running linting/formatting and tests:

```console
$ tox -e lint
$ tox -e py3
```

## Running a debug version

You can run a debug version of the app with this command:

```console
$ tox -e serve_debug
```

This will start the app at <http://localhost:7913>, and as you edit the code, the running app will reload to reflect your changes.
