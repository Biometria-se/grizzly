# Command Line Interface

`grizzly` has a command line interface that is installed separately. The command line interface is called with `grizzly-cli`, and makes it easier to start a test with all features of grizzly wrapped up nicely.

## Installation

```plain
pip install grizzly-loadtester-cli
```

## Usage

```plain
grizzly-cli [-h] [--local | --workers WORKERS]
                   [--force-build | --build] [--verbose]
                   [-T TESTDATA_VARIABLE=VALUE, [-T TESTDATA_VARIABLE=VALUE, [...]]] [-c CONFIG_FILE]
                   [file]
```

### `--local`

If `docker` and `docker-compose` are in `$PATH`, but locust should not run in distributed mode.

This parameter can not be used in combination with `--workers`, `--force-build` or `--build`.

### `--workers`

How many instances of the `workers` container that should be started.

This parameter can not be used in combination with `--local`.

### `--force-build`

If the container image for `master` and `worker` nodes should be re-built **without** cache. This takes a little time.

This parameter can not be used in combination with `--local` or `--build`.

### `--build`

If the container image for `master` and `worker` nodes should be re-built **with** cache. This is the faster option, but could result in an image that isn't correctly updated.

This parameter can not be used in combination with `--local` or `--force-build`.

### `--verbose`

Changes the log level to `DEBUG`, regardless of what it says in the feature file. Gives more verbose logging that can be useful when troubleshooting a problem with a load test scenario.

### `-T TESTDATA_VARIABLE=VALUE`

If the feature file contains the step:
```gherkin
And ask for value of ...
```

`grizzly-cli` will give you an prompt to enter the (initial) value for that variable, and you will have to confirm that the provided value is correct. With this parameter you can provide the initial value when starting the test, in which case there will not be an prompt.

### `-c CONFIG_FILE, --config-file CONFIG_FILE`

A path to an [environment configuration file](https://biometria-se.github.io/grizzly/usage/variables/environment-configuration/).
