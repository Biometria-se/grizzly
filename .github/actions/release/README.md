# release

GitHub Action to prepare a release by calculating the next version, creating a git tag, and handling tag lifecycle management.

## Description

This action automates the release preparation process for packages in a monorepo. It reads the project's configuration (`pyproject.toml` or `package.local.json`), finds the latest git tag matching the project's tag pattern, bumps the version according to semantic versioning rules, and creates a new annotated git tag. The action includes post-job cleanup to either push the tag (production) or delete it (dry-run mode).

## Inputs

| Input          | Description                                                           | Required | Default |
|----------------|-----------------------------------------------------------------------|----------|---------|
| `project`      | Project directory to release                                          | Yes      | -       |
| `version-bump` | Version bump type (`major`, `minor`, or `patch`)                      | Yes      | -       |
| `dry-run`      | Dry run mode - if `false`, push tag in cleanup; if `true`, delete tag | Yes      | -       |
| `github-token` | GitHub token for API access (used in cleanup to check job status)     | No*      | -       |

\* **Note**: While `github-token` is marked as not required in the action definition (to avoid validation warnings during the post-execution phase), it must always be provided when using this action. The token is saved to state during the main phase and retrieved during cleanup to check job status via the GitHub API.

## Outputs

| Output                 | Description                                  |
|------------------------|----------------------------------------------|
| `next-release-version` | Next release version (e.g., `1.2.3`)         |
| `next-release-tag`     | Next release tag (e.g., `framework@v1.2.3`)  |

## Usage

### Basic usage

```yaml
- name: Setup release
  id: setup
  uses: ./.github/actions/release
  with:
    project: ./framework
    version-bump: patch
    dry-run: 'false'
    github-token: ${{ secrets.GITHUB_TOKEN }}

- name: Use outputs
  run: |
    echo "Next version: ${{ steps.setup.outputs.next-release-version }}"
    echo "Next tag: ${{ steps.setup.outputs.next-release-tag }}"
```

### Dry-run mode (testing)

```yaml
- name: Setup release (dry-run)
  uses: ./.github/actions/release
  with:
    project: ./command-line-interface
    version-bump: minor
    dry-run: 'true'  # Tag will be deleted after job completes
    github-token: ${{ secrets.GITHUB_TOKEN }}
```

## Behavior

### Main action (pre-job)

1. Reads project configuration from `pyproject.toml` (Python) or `package.local.json` (Node.js)
2. Extracts git tag pattern from project configuration
3. Finds the latest existing tag matching the pattern
4. Parses the current version and bumps it according to `version-bump` type
5. Configures git user (using `GITHUB_ACTOR`)
6. Creates an annotated git tag with the new version
7. Saves state for post-job cleanup

### Post-job cleanup

- **Production mode** (`dry-run: 'false'`): Pushes the created tag to the remote repository
- **Dry-run mode** (`dry-run: 'true'`): Deletes the locally created tag

## Supported Project Types

### Python projects (pyproject.toml)

Reads tag pattern from Hatch version configuration:

```toml
[tool.hatch.version]
source = "vcs"
raw-options.scm.git.describe_command = "git describe --dirty --tags --long --match 'framework@v*[0-9]*'"
```

### Node.js projects (package.local.json)

Reads tag pattern from local package configuration:

```json
{
  "tag": {
    "pattern": "ls/vscode@v*[0-9]*"
  }
}
```

## Version Bumping

Follows semantic versioning (semver):

- **patch**: `1.2.3` → `1.2.4` (bug fixes)
- **minor**: `1.2.3` → `1.3.0` (new features, backward compatible)
- **major**: `1.2.3` → `2.0.0` (breaking changes)

## Tag Format

Tags follow the pattern: `<project-prefix>@v<version>`

Examples:
- `framework@v1.2.3`
- `cli@v2.0.0`
- `ls@v0.5.1`
- `ls/vscode@v3.5.3`

## Development

### Install dependencies

```bash
npm install
```

### Build

```bash
npm run build
```

### Test

```bash
npm test
```

### CLI mode (for testing)

```bash
node src/index.js ./framework patch
```

## Error Handling

The action will fail if:
- No recognized project file (`pyproject.toml` or `package.json`) found
- No tag pattern found in project configuration
- Invalid semantic version format
- Invalid bump type (must be `major`, `minor`, or `patch`)
- Git operations fail

## Implementation Details

- Uses **Node 24** runtime
- Implements **post-job hook** for cleanup operations
- Exports testable `getNextReleaseTag` function for unit testing
- Supports **CLI mode** for local testing and development
- Properly handles state management between pre and post phases
