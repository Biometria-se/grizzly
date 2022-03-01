#!/usr/bin/env bash

main() {
    pip-sync requirements-dev.txt --pip-args '--disable-pip-version-check --no-cache-dir --user --no-warn-script-location'
    return $?
}

main "$@"
exit $?
