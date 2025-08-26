# check-pr

GitHub Action to check pull requests for version bump labels and extract relevant information for release workflows.

## Description

This action inspects a pull request to determine if it should trigger a release by checking for version bump labels (`major`, `minor`, or `patch`). It works with both automatic triggers (when a PR is merged) and manual workflow dispatch triggers.

## Inputs

| Input          | Description                                      | Required | Default |
|----------------|--------------------------------------------------|----------|---------|  
| `pr-number`    | PR number for manual workflow_dispatch triggers  | No       | -       |
| `github-token` | GitHub token for API access                      | Yes      | -       |

## Outputs

| Output             | Description                                                  |
|--------------------|--------------------------------------------------------------|
| `should-release`   | Whether the PR should trigger a release (`true`/`false`)     |
| `version-bump`     | Version bump type (`major`, `minor`, or `patch`)             |
| `pr-number`        | PR number                                                    |
| `commit-sha`       | Merge commit SHA                                             |
| `base-commit-sha`  | Base commit SHA (the commit the PR was merged onto)          |

## Usage

### Automatic trigger (PR merge event)

```yaml
- name: Check pull request
  id: check-pr
  uses: ./.github/actions/check-pr
  with:
    github-token: ${{ secrets.GITHUB_TOKEN }}

- name: Use outputs
  if: steps.check-pr.outputs.should-release == 'true'
  run: |
    echo "Version bump: ${{ steps.check-pr.outputs.version-bump }}"
    echo "PR: #${{ steps.check-pr.outputs.pr-number }}"
```

### Manual trigger (workflow_dispatch)

```yaml
- name: Check pull request
  id: check-pr
  uses: ./.github/actions/check-pr
  with:
    pr-number: ${{ inputs.pr-number }}
    github-token: ${{ secrets.GITHUB_TOKEN }}
```

## Behavior

- **Automatic mode**: Uses the PR from the event payload (`context.payload.pull_request`)
- **Manual mode**: Fetches the PR by number using the GitHub API
- Validates that the PR is merged (only for manual mode)
- Checks for version bump labels (`major`, `minor`, `patch`) - exactly one is required
- Extracts merge commit SHA and base commit SHA
- Sets `should-release` to `false` and fails if no version label is found

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
export GITHUB_TOKEN=your_token
node src/index.js 123
```

## Error Handling

The action will fail with `should-release: false` if:
- The PR is not merged (manual mode only)
- No version bump label (`major`, `minor`, `patch`) is found on the PR
- The PR does not exist (manual mode only)
