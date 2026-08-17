# setup-xvfb

GitHub Action to run a command wrapped with Xvfb on Linux runners, for headless GUI tests.

Local replacement for [`mgor/setup-xvfb`](https://github.com/mgor/setup-xvfb) (a fork of [`coactions/setup-xvfb`](https://github.com/coactions/setup-xvfb)), which is not regularly maintained and still targets the deprecated Node 20 runtime.

On Linux runners, `xvfb` and `x11-xserver-utils` are installed and each command is wrapped with `xvfb-run --auto-servernum`. On other platforms, commands are run as-is.

## Inputs

### `run`

**Required** Command(s) to execute, one per line.

### `working-directory`

**Optional** Directory to run the command(s) in, defaults to the workspace root.

### `options`

**Optional** Xvfb server options.

## Usage

```yaml
- uses: ./.github/actions/setup-xvfb
  with:
    working-directory: ./editor-support/clients/vscode
    run: npm run test:e2e
```
