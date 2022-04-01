#!/usr/bin/env bash
set -e

main() {
    local script_dir
    local workspace
    script_dir=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
    workspace=$(mktemp -d -t ws-XXXXXX)

    pushd "${workspace}" &> /dev/null
    echo "-- creating virtualenv"
    python3 -m venv .venv &> /dev/null

    echo "-- activating virtualenv"
    source .venv/bin/activate

    echo "-- installing grizzly-cli"
    pip3 install grizzly-loadtester-cli &> /dev/null

    echo "-- generating cli.md"
    grizzly-cli --md-help > "${script_dir}/../docs/cli.md"

    echo "-- cloning repo"
    git clone https://github.com/Biometria-se/grizzly-cli.git

    # @TODO: maybe check which version of grizzly-loadtester-cli is installed, and checkout that tag

    echo "-- generating changelog/grizzly-cli"
    pushd "grizzly-cli/" &> /dev/null
    python3 "$1/script/docs-generate-changelog.py" --from-directory "$PWD"

    echo "-- installing grizzly-cli dev dependencies"
    python3 -m pip install .[dev]

    echo "-- generating licenses/grizzly-loadtester-cli.md"
    python3 script/docs-generate-licenses.py > "$1/docs/licenses/grizzly-loadtester-cli.md"
    popd &> /dev/null

    echo "-- cleaning up"
    deactivate
    popd &> /dev/null
    rm -rf "${workspace}"

    return 0
}

main "$@"
exit $?
