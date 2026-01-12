# grizzly-monorepo

this is a monorepo for all grizzly related projects, in the form of a [uv workspace](https://docs.astral.sh/uv/concepts/projects/workspaces/)
the language is python, the package manager is [uv](https://docs.astral.sh/uv/) and the build system is [hatchling](https://hatch.pypa.io/latest/).

linting and formatting is done with [ruff](https://docs.astral.sh/ruff/).

**Grizzly is a framework to be able to easily define load scenarios, and is primarily built on-top of two other frameworks.**

> [Locust](https://locust.io): Define user behaviour with Python code, and swarm your system with millions of simultaneous users.

> [Behave](https://behave.readthedocs.io/): Uses tests written in a natural language style, backed up by Python code.

**`behave` is <del>ab</del>used for being able to define `locust` load test scenarios using [gherkin](https://cucumber.io/docs/gherkin). A feature can contain more than one scenario and all scenarios will run in parallell. This makes it possible to implement load test scenarios without knowing python or how to use `locust`.**

the repository is hosted on [github](https://github.com).

all code should be pythonic and follow [PEP-8](https://peps.python.org/pep-0008/) as much as possible.

all code must be covered with unit tests, using [pytest](https://docs.pytest.org/en/latest/), and when possible also have end-to-end tests to validate the full functionality.

builds should be reproducible, meaning that the same source code should always produce the same build output, this is accomplished by using `uv` with a `lock` file, and `--locked` when syncing
dependencies.

## Development Workflows

### Testing
```bash
# Run unit tests for a package
hatch run test:{package}-unit

# Run e2e tests for a package 
hatch run test:{package}-e2e

# Run all tests for a package
hatch run test:{package}-all
```

### Code Quality
```bash
# Format code
hatch run lint:format

# Run type checking
hatch run lint:types

# Run linting
hatch run lint:check
```

### Dependency Management
```bash
# Install dependencies (preserving lock file)
uv sync --locked -p {python-version} --active --all-packages --all-groups
```

### Terminal Usage
**IMPORTANT**: Always activate the Python virtual environment when starting a new terminal session:
```bash
source /workspaces/grizzly/.venv/bin/activate
```

This is required for:
- Running Python commands and scripts
- Executing e2e tests (especially for VSCode extension which depends on `grizzly_ls`)
- Using `hatch` commands
- Any operation that imports grizzly packages

## Key Development Patterns

1. Package Dependencies:
   - Keep shared code in `grizzly-loadtester-common`
   - Use workspace dependencies in `pyproject.toml`
   - Always preserve `uv.lock` file

2. Testing Strategy:
   - Unit tests mock external dependencies
   - E2E tests run against real services
   - Group tests by functionality

## Common Gotchas
1. Always use `uv sync --locked` to preserve dependency locks
2. E2E tests require additional setup (see package READMEs)
3. VS Code extension requires rebuild after language server changes

## packages

### [grizzly-loadtester-common](../common) - common code shared between the other packages

this package contains code that is shared between the other packages, like custom exceptions, constants and utility functions.
there should not be any duplicated code in the other packages, if so it should be moved to this package.

### [grizzly-loadtester](../framework) - the core framework

this package contains the corner stones of the grizzly framework, like the custom locust messages, the test data producer/consumer and the gherkin parser.
load users that tests different protocols or services.

### [grizzly-loadtester-cli](../command-line-interface) - command line interface

this package contains the command line interface for grizzly, that is used to run the load tests. it is used to start grizzly, either in local mode or in distributed mode with a master and multiple workers.

it also contains utilities such as Azure Keyvault integration (store sensitiv environment variables in a secure way), a TOTP generator, and a utility to generate the project structure of a new grizzly project.

### [grizzly-loadtester-ls](../editor-support) - language server for editor support, using the [LSP protocol](https://microsoft.github.io/language-server-protocol/)

this is the language server with all the logic of validating feature files, step definitions and providing auto-completion. it is built using [pygls](https://github.com/openlawlibrary/pygls).
for common problems there should be a quick fix available.

in theory, the language server should work with any feature files that is using behave, but the main focus is to support grizzly features.

#### [vscode extension](../editor-support/clients/vscode) - visual studio code extension

this is the visual studio code extension that provides the integration with the language server. in general is does not contain any logic, only integrations towards [grizzly-loadtester-ls](../editor-support) features.

**Important**: When updating `@types/vscode` in `package.json`, the `engines.vscode` field must be updated to match the same major.minor version. For example, if `@types/vscode` is `1.108.1`, then `engines.vscode` should be `^1.108.0`.

### [grizzly-docs](../docs) - documentation

the mkdocs based documentation for grizzly, hosted on [github pages](https://biometria-se.github.io/grizzly/). parts are static, and parts are dynamic generated from the code base with [mkdocstrings](https://mkdocstrings.github.io/).

a custom plugin is used to generate the API reference for the packages and where to insert them in the navigation, and a custom theme is used to provide a better user experience.
