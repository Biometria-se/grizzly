#!/usr/bin/env bash
set -e

main() {
    local what="$1"
    local script_dir
    local workspace
    local cwd="${PWD}"
    script_dir=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
    workspace=$(mktemp -d -t ws-XXXXXX)

    pushd "${workspace}" &> /dev/null
    >&2 echo "-- creating virtualenv"
    python3 -m venv .venv &> /dev/null

    >&2 echo "-- activating virtualenv"
    source .venv/bin/activate
    >&2 echo "-- installing grizzly-cli"
    pip3 install grizzly-loadtester-cli[dev] &> /dev/null

    case "${what}" in
        --usage)

            >&2 echo "-- generating usage"
            grizzly-cli --md-help
            ;;
        --licenses|--changelog)
            >&2 echo "-- cloning repo"
            git clone https://github.com/Biometria-se/grizzly-cli.git

            pushd "grizzly-cli/" &> /dev/null

            local version
            version="$(grizzly-cli --version | awk '{print $NF}')"
            >&2 echo "checking out tag v${version}"
            git checkout "tags/v${version}" -b "v${version}"

            case "${what}" in
                --changelog)
                    >&2 echo "-- generating changelog: $PWD, $cwd"
                    python3 "${cwd}/script/docs-generate-changelog.py" --from-directory "$PWD"
                    ;;
                --licenses)
                    >&2 echo "-- generating licenses"
                    python3 script/docs-generate-licenses.py
                    ;;
            esac
            popd &> /dev/null
            ;;
        *)
            echo "unknown argument: ${what}"
            return 1
            ;;
    esac

    >&2 echo "-- cleaning up"
    deactivate || true
    popd &> /dev/null
    rm -rf "${workspace}" || true

    return 0
}

main "$@"
exit $?
