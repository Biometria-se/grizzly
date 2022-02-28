#!/usr/bin/env bash

main() {
    export CUSTOM_COMPILE_COMMAND="script/pip-generate-requirements.bash"

    # generate requirements.txt
    pip-compile --generate-hashes --allow-unsafe || return 1

    # generate requirements-ci.txt
    pip-compile --generate-hashes --allow-unsafe --extra dev -o requirements-ci.txt || return 1

    # generate requirements-dev.txt
    pip-compile --generate-hashes --allow-unsafe --extra mq --extra dev -o requirements-dev.txt || return 1

    return 0
}

main "$@"
exit $?
