#!/usr/bin/env bash

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

    echo "-- cleaning up"
    deactivate
    popd &> /dev/null
    rm -rf "${workspace}"

    return 0
}

main "$@"
exit $?
