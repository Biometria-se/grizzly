# Development Guide

This guide covers everything you need to know to develop in the [grizzly repository](https://github.com/biometria-se/grizzly).

## Repository Structure

This is a monorepo for all Grizzly-related projects, managed as a [uv workspace](https://docs.astral.sh/uv/concepts/projects/workspaces/).

- **Language**: Python
- **Package Manager**: [uv](https://docs.astral.sh/uv/)
- **Build System**: [hatchling](https://hatch.pypa.io/latest/)
- **Linting & Formatting**: [ruff](https://docs.astral.sh/ruff/)
- **Testing**: [pytest](https://docs.pytest.org/en/latest/)

### Packages

1. **[grizzly-loadtester-common](../../common/)** - Shared code between packages (exceptions, constants, utilities)
2. **[grizzly-loadtester](../../framework/)** - Core framework with locust messages, test data producer/consumer, gherkin parser, and load users
3. **[grizzly-loadtester-cli](../../command-line-interface/)** - Command line interface for running tests, Azure Keyvault integration, TOTP generator, project scaffolding
4. **[grizzly-loadtester-ls](../../editor-support/)** - Language server implementation using [LSP protocol](https://microsoft.github.io/language-server-protocol/)
5. **[vscode extension](../../editor-support/clients/vscode/)** - Visual Studio Code extension integrating the language server
6. **[grizzly-docs](../../docs/)** - MkDocs-based documentation hosted on [GitHub Pages](https://biometria-se.github.io/grizzly/)

## Setting Up Development Environment

### Prerequisites

- Python 3.10 or higher
- [uv](https://docs.astral.sh/uv/) package manager
- Node.js 24 (for VS Code extension development)
- Git

### Installing Dependencies

Install all dependencies for all packages (preserving the lock file):

```bash
uv sync --locked -p 3.13 --active --all-packages --all-groups
```

> **Important**: Always use `--locked` to ensure reproducible builds by preserving the dependency lock file.

### Development Container

This repository includes a dev container configuration for VS Code, which provides a pre-configured development environment with all necessary tools.

## Running Tests

The repository uses `hatch` environments for running tests across different Python versions.

### Unit Tests

Run unit tests for a specific package:

```bash
# Framework
hatch run test:framework-unit

# CLI
hatch run test:cli-unit

# Language Server
hatch run test:ls-unit

# Common
hatch run test:common-unit

# Docs
hatch run test:docs-unit
```

### End-to-End Tests

Run e2e tests for packages that support them:

```bash
# Framework (local mode)
hatch run test:framework-e2e-local

# Framework (distributed mode)
hatch run test:framework-e2e-dist

# Framework (example tests)
hatch run test:framework-e2e-example

# CLI
hatch run test:cli-e2e

# Language Server
hatch run test:ls-e2e
```

### All Tests

Run all tests (unit + e2e) for a package:

```bash
hatch run test:framework-all
hatch run test:cli-all
hatch run test:ls-all
```

### Test Matrix

Tests run across multiple Python versions (3.10, 3.11, 3.12, 3.13). Use the matrix syntax:

```bash
hatch run +py=3.13 test:framework-unit
```

## Code Quality

### Formatting

Format all code using ruff:

```bash
hatch run lint:format
```

### Type Checking

Run mypy type checking:

```bash
hatch run lint:types
```

### Linting

Run ruff linting checks:

```bash
hatch run lint:check
```

## Development Patterns

### Code Style

- All code should be Pythonic and follow [PEP-8](https://peps.python.org/pep-0008/)
- Use type hints where appropriate
- Write descriptive docstrings for public functions and classes

### Package Dependencies

1. **Keep shared code in `grizzly-loadtester-common`**
   - Don't duplicate code across packages
   - Move common utilities to the common package

2. **Use workspace dependencies in `pyproject.toml`**
   ```toml
   [project]
   dependencies = [
       "grizzly-loadtester-common",
   ]
   ```

3. **Always preserve `uv.lock`**
   - Use `uv sync --locked` when installing
   - Commit the lock file to ensure reproducible builds

### Testing Strategy

1. **Unit Tests**: Mock external dependencies, fast execution
2. **E2E Tests**: Run against real services, validate full functionality
3. **Coverage**: All code must be covered with unit tests
4. **Group Tests**: Organize tests by functionality

### Adding a New Package

When adding a new package to the workspace:

1. Create the package directory with `pyproject.toml`
2. Update root `pyproject.toml`:
   - `tool.pytest.ini_options`
   - `tool.coverage.path`
   - `tool.mypy`
3. Add test scripts to `tool.hatch.envs.test.scripts`
4. Run `uv sync --locked` to update the lock file

## Release Process

Releases are automated via the `.github/workflows/release.yaml` workflow.

### Release Triggers

The release workflow can be triggered in two ways:

1. **Automatic (PR Merge)**: When a PR is merged to `main` with one of these labels:
   - `major` - Breaking changes (X.0.0)
   - `minor` - New features (0.X.0)
   - `patch` - Bug fixes (0.0.X)

2. **Manual (Workflow Dispatch)**: Manually trigger the workflow from GitHub Actions UI
   - Specify the PR number
   - Optionally enable dry-run mode (no actual release)

### Release Workflow Steps

1. **Prerequisites Job**
   - Validates the PR and checks for the version bump label
   - Detects which packages changed using `.github/changes-filter.yaml`
   - Maps changes to packages and their dependencies

2. **UV Release Job** (Python packages)
   - Builds packages using `uv build --package <package> --sdist --wheel`
   - Creates a git tag (e.g., `framework@v1.2.3`)
   - Publishes to PyPI using trusted publishing (OIDC)
   - Pushes the tag only if all steps succeed

3. **NPM Release Job** (VS Code extension)
   - Builds the extension using `@vscode/vsce package`
   - Creates a git tag (e.g., `vscode-extension@v1.2.3`)
   - Publishes to VS Code Marketplace 
   - Pushes the tag only if all steps succeed

4. **Documentation Job**
   - Builds and deploys documentation to GitHub Pages
   - Only runs if previous jobs succeed

### Version Management

Versions are managed automatically using:

- **Python packages**: `hatch-vcs` with git tags matching pattern in `pyproject.toml`
  ```toml
  [tool.hatch.version]
  source = "vcs"
  raw-options.scm.git.describe_command = "git describe --tags --match 'framework@v*[0-9]*'"
  ```

- **NPM packages**: `package.local.json` defines the tag pattern
  ```json
  {
    "tag": {
      "pattern": "ls/<IDE>@v*[0-9]*"
    }
  }
  ```

### Release Requirements

For a package to be releasable:

1. **Python packages** must have:
   - `tool.hatch.version.raw-options.scm.git.describe_command` in `pyproject.toml`

2. **NPM packages** must have:
   - `tag.pattern` in `package.local.json`

3. **Workflow files** (.github/workflows/) cannot be modified in the same PR
   - The `map-changes.py` script will fail if workflow changes are detected during release
   - This prevents permission issues with `GITHUB_TOKEN` when pushing tags

### Tag Management

The `setup-release` action creates tags during the job and manages them in a cleanup phase:

- **Success**: Tag is pushed to the repository
- **Failure**: Tag is deleted locally (not pushed)
- **Dry-run**: Tag is deleted (no push)

### Secrets Required

Configure these secrets in your GitHub repository:

1. **`VSCE_TOKEN`**: Personal Access Token for VS Code Marketplace
   - Required for publishing the VS Code extension
   - Get from [Azure DevOps](https://dev.azure.com/)

2. **`GITHUB_TOKEN`**: Automatically provided by GitHub Actions
   - Used for API access and documentation deployment

### Manual Release Example

```bash
# Trigger via GitHub Actions UI
# 1. Go to Actions → Release
# 2. Click "Run workflow"
# 3. Select branch: main
# 4. Enter PR number: 123
# 5. Enable dry-run: true (optional)
# 6. Click "Run workflow"
```

### Dry-Run Mode

Test the release process without actually publishing:

```bash
# In workflow_dispatch, set dry-run: true
```

This will:
- Create and validate tags
- Build packages
- Skip actual publishing to PyPI/VS Code Marketplace
- Delete tags at the end

## VS Code Extension Development

### Building the Extension

```bash
cd editor-support/clients/vscode
npm install
npm run compile
```

### Watch Mode

For active development with auto-rebuild:

```bash
npm run tsc-watch
```

Or use the VS Code task:

```bash
# Press Cmd/Ctrl+Shift+B → Select "tsc watch"
```

### Testing the Extension

1. Open the extension project in VS Code
2. Press `F5` to launch Extension Development Host
3. The extension will be loaded in a new VS Code window

### Important Notes

- VS Code extension requires rebuild after language server changes
- Always run tests after modifying language server code
- Extension source is in TypeScript, compiled to JavaScript in `dist/`

## Documentation

The documentation is built using MkDocs with a custom plugin for API reference generation.

### Building Documentation

```bash
uv run mkdocs build
```

### Serving Documentation Locally

```bash
uv run mkdocs serve
```

Then visit [http://localhost:8000](http://localhost:8000)

### Documentation Structure

- `docs/site/` - Static markdown pages
- `docs/build/` - Generated output (deployed to GitHub Pages)
- Custom MkDocs plugin generates API reference from code

## Common Gotchas

1. **Always use `uv sync --locked`**
   - Preserves dependency locks
   - Ensures reproducible builds

2. **E2E tests require additional setup**
   - See package-specific READMEs
   - May need external services (IBM MQ, Azure services, etc.)

3. **VS Code extension rebuild**
   - Required after language server changes
   - Run `npm run compile` or use watch mode

4. **Python version compatibility**
   - Test across all supported versions (3.10-3.13)
   - Use `hatch` matrix environments

5. **Workflow file changes**
   - Cannot be released in the same PR as package changes
   - Will cause the release workflow to fail
   - Create separate PRs for workflow and package changes

## Troubleshooting

### Dependency Issues

If you encounter dependency conflicts:

```bash
# Remove the lock file and resync
rm uv.lock
uv sync -p 3.13 --active --all-packages --all-groups
```

### Test Failures

1. Check if dependencies are installed: `uv sync --locked`
2. Verify Python version: `python --version`
3. Check for environment-specific issues in package READMEs
4. Run with verbose output: `pytest -vv`

### Release Failures

1. Check PR has correct version label (major/minor/patch)
2. Verify secrets are configured (VSCE_TOKEN)
3. Ensure workflow files aren't modified in the PR
4. Check GitHub Actions logs for detailed error messages
5. Verify package has release configuration (tag pattern in pyproject.toml or package.local.json)

## Contributing

1. Create a feature branch from `main`
2. Make your changes following the development patterns
3. Write tests for new functionality
4. Run linting and type checking
5. Create a PR with appropriate labels (major/minor/patch for releases)
6. Ensure all CI checks pass

## Additional Resources

- [Grizzly Documentation](https://biometria-se.github.io/grizzly/)
- [uv Documentation](https://docs.astral.sh/uv/)
- [hatch Documentation](https://hatch.pypa.io/latest/)
- [pytest Documentation](https://docs.pytest.org/)
- [ruff Documentation](https://docs.astral.sh/ruff/)
- [Language Server Protocol](https://microsoft.github.io/language-server-protocol/)
