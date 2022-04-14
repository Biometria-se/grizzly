#!/usr/bin/env bash
set -e

main() {
    local script_dir
    local workspace
    local cwd="${1:-${PWD}}"
    script_dir=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
    workspace=$(mktemp -d -t ws-XXXXXX)

    pushd "${workspace}" &> /dev/null
    echo "-- creating virtualenv"
    python3 -m venv .venv &> /dev/null

    echo "-- activating virtualenv"
    source .venv/bin/activate

    echo "-- installing grizzly-cli"
    pip3 install grizzly-loadtester-cli[dev] &> /dev/null

    echo "-- generating cli.md"
    grizzly-cli --md-help > "${script_dir}/../docs/cli.md"

    echo "-- cloning repo"
    git clone https://github.com/Biometria-se/grizzly-cli.git

    # @TODO: maybe check which version of grizzly-loadtester-cli is installed, and checkout that tag

    echo "-- generating changelog/grizzly-cli"
    pushd "grizzly-cli/" &> /dev/null
    python3 "${cwd}/script/docs-generate-changelog.py" --from-directory "$PWD"

    echo "-- generating licenses/grizzly-loadtester-cli.md"
    [[ -d "${cwd}/docs/licenses" ]] || mkdir -p "${cwd}/docs/licenses"
    python3 script/docs-generate-licenses.py > "${cwd}/docs/licenses/grizzly-loadtester-cli.md"
    popd &> /dev/null

    echo "-- cleaning up"
    deactivate || true
    popd &> /dev/null
    rm -rf "${workspace}" || true

    return 0
}

main "$@"
exit $?
