# set-timezone

GitHub Action to set the runner's timezone based on its operating system.

Local replacement for [`MathRobin/timezone-action`](https://github.com/MathRobin/timezone-action) (a fork of [`szenius/set-timezone`](https://github.com/szenius/set-timezone)), which is not regularly maintained and still targets the deprecated Node 20 runtime.

## Inputs

### `timezoneLinux`

**Optional** Timezone to set on Linux runners. Default `UTC`.

### `timezoneWindows`

**Optional** Timezone to set on Windows runners. Default `UTC`.

### `timezoneMacos`

**Optional** Timezone to set on macOS runners. Default `GMT`.

## Usage

```yaml
- uses: ./.github/actions/set-timezone
  with:
    timezoneLinux: "Europe/Stockholm"
    timezoneWindows: "W. Europe Standard Time"
    timezoneMacos: "Europe/Stockholm"
```
