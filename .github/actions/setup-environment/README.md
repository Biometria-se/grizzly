# setup-environment

GitHub Action to setup environment variables and PATH for GitHub Actions workflows.

## Description

This action configures environment variables and PATH entries for GitHub Actions workflows. It can be used with custom values or will automatically configure a Python virtual environment when no inputs are provided.

## Inputs

### `add-env`

**Optional** Comma-separated list of environment variables to add in `key=value` format.

### `add-path`

**Optional** Comma-separated list of paths to add to the PATH variable.

## Usage

### Default behavior (Python virtual environment)

When no inputs are provided, the action sets up a Python virtual environment:

```yaml
- uses: ./.github/actions/setup-environment
```

This configures:
- `VIRTUAL_ENV`: Path to `.venv` in the workspace
- `GRIZZLY_TMP_DIR`: System temporary directory
- `GRIZZLY_TMP_LOGFILE`: Path to `grizzly.log` in temp directory
- Adds virtual environment's `bin` (or `Scripts` on Windows) to PATH

### Custom environment variables

```yaml
- uses: ./.github/actions/setup-environment
  with:
    add-env: 'MY_VAR=value,ANOTHER_VAR=123'
```

### Custom PATH entries

```yaml
- uses: ./.github/actions/setup-environment
  with:
    add-path: '/custom/bin,/another/path'
```

### Both custom variables and paths

```yaml
- uses: ./.github/actions/setup-environment
  with:
    add-env: 'DATABASE_URL=postgres://localhost'
    add-path: '/usr/local/bin'
```

## Special Handling

### LD_LIBRARY_PATH

When setting `LD_LIBRARY_PATH`, the action automatically appends to any existing value using the appropriate path separator (`:` on Linux/macOS, `;` on Windows).

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
