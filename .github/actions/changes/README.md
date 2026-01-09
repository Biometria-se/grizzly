# changes

GitHub Action to map changed directories to package changes and their test configurations.

## Description

This action analyzes the repository structure to determine which packages have changed and identifies their corresponding test suites. It supports both Python (uv-managed) and Node.js (npm-managed) packages, including reverse dependencies.

## Inputs

### `changes`

**Required** JSON string of list of directories that had changes.

### `force`

**Required** Force run on all packages (true/false). When true, analyzes all packages from `changes-filter.yaml`.

### `release`

**Optional** Indicates if this is a release run. Only packages with release configuration will be included. Default: `false`.

## Outputs

### `changes_uv`

JSON string of Python (uv) package changes with test configurations.

### `changes_npm`

JSON string of npm package changes with test configurations.

## Usage

### Basic usage

```yaml
- name: Map changes
  id: changes
  uses: ./.github/actions/changes
  with:
    changes: '${{ steps.filter.outputs.changes }}'
    force: 'false'

- name: Use mapped changes
  run: echo '${{ steps.changes.outputs.changes_uv }}'
```

### Force mode (all packages)

```yaml
- name: Map all packages
  id: changes
  uses: ./.github/actions/changes
  with:
    changes: '[]'
    force: 'true'
```

### Release mode

```yaml
- name: Map changes for release
  id: changes
  uses: ./.github/actions/changes
  with:
    changes: '${{ steps.filter.outputs.changes }}'
    force: 'false'
    release: 'true'
```

## Output Format

Each change object contains:

```json
{
  "directory": "framework",
  "package": "grizzly-loadtester",
  "tests": {
    "unit": "tests/test_framework/unit",
    "e2e": {
      "local": "tests/test_framework/e2e",
      "dist": "tests/test_framework/e2e"
    }
  }
}
```

## Features

- **Python Package Detection**: Reads `pyproject.toml` for package metadata
- **Node Package Detection**: Reads `package.json` for package metadata
- **Reverse Dependencies**: Automatically includes packages that depend on changed packages
- **Release Mode**: Filters packages based on release configuration
- **Test Detection**: Automatically detects unit and e2e test directories

## CLI Mode

The action can be run locally in CLI mode:

```bash
cd .github/actions/changes
node src/index.js --changes '["framework"]' --force false
node src/index.js --changes '[]' --force true --release
```

## Development

### Install dependencies

```bash
npm install
```

### Run tests

```bash
npm test
```

### Lint code

```bash
npm run lint
```

### Build

```bash
npm run build
```

## License

MIT
