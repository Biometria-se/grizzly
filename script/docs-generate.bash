#!/usr/bin/env bash
set -e

main() {
    local target="${1:-cli}"
    local what="$2"
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

    local pypi_suffix

    case "${target}" in
        cli)
            pypi_suffix="${target}"
            ;;
        lsp)
            pypi_suffix="${target:0:2}"
            ;;
        *)
            if [[ ! -z "${target}" ]]; then
                echo "unknown argument: ${target}"
                return 1
            fi
            ;;
    esac

    >&2 echo "-- installing grizzly-${pypi_suffix}"
    if ! python -m pip install --no-cache-dir grizzly-loadtester-${pypi_suffix}[dev] 2>&1 > /tmp/grizzly-cli-install.log; then
        cat /tmp/grizzly-cli-install.log
    fi

    rm -rf /tmp/grizzly-cli-install.log || true

    case "${what}" in
        --usage)
            if [[ "${target}" != "cli" ]]; then
                >&2 echo "not valid for ${target}"
                return 1
            fi
            >&2 echo "-- generating usage"
            grizzly-${target} --md-help
            rc=$(( rc + $? ))
            ;;
        --licenses|--changelog)
            >&2 echo "-- cloning repo"
            git clone https://github.com/Biometria-se/grizzly-${target}.git
            rc=$(( rc + $? ))

            pushd "grizzly-${target}/" &> /dev/null

            local tag
            if [[ "${target}" == "cli" ]]; then
                local version
                version="$(grizzly-${target} --version | awk '{print $NF}')"
                rc=$(( rc + $? ))
                tag="v${version}"
            else
                tag="$(git tag | grep -E '^v' | sort --version-sort | tail -1)"
            fi

            >&2 echo "checking out tag ${tag}"
            git checkout "tags/${tag}" -b "${tag}"
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
