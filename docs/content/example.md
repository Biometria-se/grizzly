---
title: Example
---
@anchor framework.example Example
# Example

The directory `example/` is an working project that sends requests to public REST API endpoints, **please do not abuse these**.

## Structure

The project *must* have the follwoing structure:

```plain
.
└── features
    ├── environment.py
    ├── test.feature
    ├── requests
    │   └── ...
    └── steps
        └── steps.py
```

In this example there are two `requirements*.txt` files. The reason is that `requirements.txt` will be copied and installed in the container image if `grizzly-cli` is used.
The container image **should not** contain `grizzly-cli` and should be installed where scenarios are started from.

After installing `grizzly-cli` the easiest way to get a correct project structure is to use the builtin `init` subcommand:

```bash
grizzly-cli init my-grizzly-project
cd my-grizzly-project/
```

### Environment

`features/environment.py` *should* contain:

```python
from grizzly.environment import *
```

This file can contain overloading of `behave` hooks to trigger events that should happen during different stages of running a feature file.

```python
from grizzly.environment import before_feature as grizzly_before_feature, after_feature as grizzly_after_feature, before_scenario, after_scenario, before_step

def before_feature(context: Context, *args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> None:
    # custom code that should run before feature file is started, e.g. notify something that a test
    # is started

    grizzly_before_feature(context, *args, **kwargs)


def after_feature(context: Context, feature: Feature, *args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> None:
    grizzly_after_feature(context, feature, *args, **kwargs)

    # custom code that should run before feature file is started, e.g. notify something that a test
    # is finished
```

### Steps

`features/steps/steps.py` *should* contain:

```python
from grizzly.steps import *
```

This is where custom step implementation can be added, then should look something like:

```python
from behave.runner import Context
from behave import then  # pylint: disable=no-name-in-module

from grizzly.steps import *
from grizzly.context import GrizzlyContext


@then(u'this custom step should be executed')
def step_custom_the_custom_step(context: Context) -> None:
    grizzly = cast(GrizzlyContext, context.grizzly)

    # custom step implementation
```

### Request templates

`features/requests` can contain jinja2 templates used in requests. E.g., if the feature file contains the following step:

```gherkin
Then send request "payload.j2.json"
```

Then `features/requests/payload.j2.json` needs to exist.

## Get

First do a sparse checkout of the `example/` directory in the repository.

If you have `git` older than `2.25.0`, follow these [instructions on stackoverflow.com](https://stackoverflow.com/a/13738951/3378455).

=== "Bash"

    ```bash
    mkdir grizzly-example
    cd grizzly-example
    git init
    git remote add -f origin https://github.com/Biometria-se/grizzly.git
    git sparse-checkout init
    git sparse-checkout set example/
    git pull origin main
    rm -rf .git/
    cd example/
    ```

=== "PowerShell"

    ```powershell
    mkdir grizzly-example
    cd .\grizzly-example\
    git init
    git remote add -f origin https://github.com/Biometria-se/grizzly.git
    git sparse-checkout init
    git sparse-checkout set example/
    git pull origin main
    rm -Recurse -Force .\.git\
    cd .\example\
    ```

Create an python virtual environment and install dependencies:

=== "Bash"

    ```bash
    python3 -m venv .env
    source .env/bin/activate
    pip3 install -r requirements.txt
    pip3 install grizzly-loadtester-cli
    ```

=== "PowerShell"

    ```powershell
    python3 -m venv .env
    .\.env\Scripts\activate
    pip3 install -r .\requirements.txt
    pip3 install grizzly-loadtester-cli
    ```


If you do not already have an working "IBM MQ" client setup and run `grizzly-cli` in local mode you will not be able to use `MessageQueueUser`. See [`grizzly-cli/static/Containerfile`](https://github.com/Biometria-se/grizzly-cli/blob/main/grizzly_cli/static/Containerfile#L27-L36) on how to get these. When that is done you need to install the extra dependencies:

```bash
pip3 install grizzly-loadtester[mq]
```

## Run

`grizzly` has some runtime features which is easiliest handled by using the `grizzly-cli`. It provides a simple command line interface wrapping the `behave` command, for providing initial variable values, configuration etc.

To run the example, in local mode:

=== "Bash"

    ```bash
    grizzly-cli local run -e environments/example.yaml features/example.feature
    ```

=== "PowerShell"

    ```powershell
    grizzly-cli local run -e .\environments\example.yaml .\features\example.feature
    ```

And in distributed mode (requires `docker` and `docker-compose` in `PATH`):

=== "Bash"

    ```bash
    grizzly-cli dist run -e environments/example.yaml features/example.feature
    ```

=== "PowerShell"

    ```powershell
    grizzly-cli dist run -e .\environments\example.yaml .\features\example.feature
    ```

## Develop

If you have [Visual Studio Code](https://code.visualstudio.com/download) installed, you can also install the grizzly extension to make your life easier when developing scenarios!

```bash
pip3 install grizzly-loadtester-ls
code --install-extension biometria-se.grizzly-loadtester-vscode
```

