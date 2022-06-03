#!/usr/bin/env bash
set -e

main() {
    local what="$1"
    local script_dir
    local workspace
    local cwd="${PWD}"
    local rc=0

    script_dir=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
    workspace=$(mktemp -d -t ws-XXXXXX)

    pushd "${workspace}" &> /dev/null
    >&2 echo "-- creating virtualenv"
    python3 -m venv .venv &> /dev/null

    >&2 echo "-- activating virtualenv"
    source .venv/bin/activate
    >&2 echo "-- installing grizzly-cli"
    pip3 install --no-cache-dir grizzly-loadtester-cli[dev] &> /dev/null
    rc=$(( rc + $? ))

    case "${what}" in
        --usage)

            >&2 echo "-- generating usage"
            grizzly-cli --md-help
            rc=$(( rc + $? ))
            ;;
        --licenses|--changelog)
            >&2 echo "-- cloning repo"
            git clone https://github.com/Biometria-se/grizzly-cli.git
            rc=$(( rc + $? ))

            pushd "grizzly-cli/" &> /dev/null

            local version
            version="$(grizzly-cli --version | awk '{print $NF}')"
            rc=$(( rc + $? ))

            >&2 echo "checking out tag v${version}"
            git checkout "tags/v${version}" -b "v${version}"
            rc=$(( rc + $? ))

            case "${what}" in
                --changelog)
                    >&2 echo "-- generating changelog: $PWD, $cwd"
                    python3 "${cwd}/script/docs-generate-changelog.py" --from-directory "$PWD"
                    rc=$(( rc + $? ))
                    ;;
                --licenses)
                    >&2 echo "-- generating licenses"
                    python3 script/docs-generate-licenses.py
                    rc=$(( rc + $? ))
                    ;;
            esac
            popd &> /dev/null
            ;;
        *)
            if [[ ! -z "${what}" ]]; then
                echo "unknown argument: ${what}"
                rc=1
            fi
            ;;
    esac

    >&2 echo "-- cleaning up"
    deactivate || true
    popd &> /dev/null
    rm -rf "${workspace}" || true

    return $rc
}

main "$@"
exit $?
