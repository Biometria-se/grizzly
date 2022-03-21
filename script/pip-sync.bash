#!/usr/bin/env bash

set -e

main() {
    local version="${1:-dev}"
    case "${version}" in
        dev|prod|ci)
            ;;
        *)
            >&2 echo "unknown version ${version}: choose one of prod, dev or ci"
            return 1
    esac

    local suffix
    if [[ "${version}" == "prod" ]]; then
        suffix=""
    else
        suffix="-${version}"
    fi

    local requirements_file="requirements${suffix}.txt"

    if [[ ! -e "${requirements_file}" ]]; then
        >&2 echo "${requirements_file} does not exist in ${PWD}"
        return 1
    fi

    pip-sync "${requirements_file}" --pip-args '--disable-pip-version-check --no-cache-dir --user --no-warn-script-location --no-deps'
    return $?
}

main "$@"
exit $?
