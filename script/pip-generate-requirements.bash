#!/usr/bin/env bash

# @TODO: this should probably be re-written in python

main() {
    local mode="${1}"

    if [[ "${mode}" == "generate" ]]; then
        local pip_args='--disable-pip-version-check --no-cache-dir --user --no-warn-script-location'
        #local pip_compile_args="-q --generate-hashes --allow-unsafe"
        local pip_compile_args="-q --allow-unsafe"
        local arguments

        cd /mnt
        export CUSTOM_COMPILE_COMMAND="script/pip-generate-requirements.bash"

        declare -A files
        declare -a pids
        files[requirements.txt]=""
        files[requirements-ci.txt]="--extra dev -o requirements-ci.txt"
        files[requirements-dev.txt]="--extra dev --extra mq -o requirements-dev.txt"

        for file in "${!files[@]}"; do
            arguments="${files[${file}]}"
            >&2 echo "generating ${file}"
            python3 -m piptools compile ${pip_compile_args} --pip-args "${pip_args}" ${arguments} &
            pids+=($!)
        done

        for pid in "${pids[@]}"; do
            wait "${pid}"
        done
    else
        if !type docker &>/dev/null; then
            >&2 echo "docker not found in $PATH"
            return 1
        fi

        local cwd
        local python_version
        local user_uid
        local user_gid
        user_gid="$(id -g)"
        user_uid="$(id -u)"

        cwd=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &> /dev/null && pwd)
        python_version=$(python3 - <<'EOF'
import sys, ast, re, packaging.version as pv
with open('setup.py') as f:
    r = [n.value.value
        for n in ast.walk(ast.parse(f.read()))
            if isinstance(n, ast.keyword) and
                n.arg == 'python_requires' and
                isinstance(n.value, ast.Constant)
    ][-1]
v = re.sub(r'[^0-9\.]', '', r)
p = pv.parse(v)
m = p.minor
if '>' in r:
    c, s = (p.__le__ if '=' in r else p.__lt__, 1, )
elif '<' in r:
    c, s = (p.__ge__ if '=' in r else p.__gt__, -1, )
else:
    c, s = (p.__eq__, 0, )
while not c(pv.parse(f'{p.major}.{m}')) and (m > 0 or m < p.minor * 2):
    m += s
print(f'{p.major}.{m}', end='')
sys.exit(0 if m > 0 and m < p.minor * 2 else 1)
EOF
) || return 1

        echo "running pip-compile with python ${python_version}"

        # Container file inline, so not one extra file is needed, yes we know about --build-arg
        docker image build -t pip-generate-requirements:latest ${cwd} -q -f-<<EOF
# this is need because pip-compile needs them (!!) when compiling requirements*.txt file, with pymqi included
FROM alpine:latest as dependencies
USER root
RUN mkdir /root/ibm && cd /root/ibm && \
    wget https://public.dhe.ibm.com/ibmdl/export/pub/software/websphere/messaging/mqdev/redist/9.2.2.0-IBM-MQC-Redist-LinuxX64.tar.gz -O - | tar xzf -
FROM python:${python_version}-alpine
RUN mkdir -p /opt/mqm/lib64 && mkdir /opt/mqm/lib && mkdir -p /opt/mqm/gskit8/lib64
COPY --from=dependencies /root/ibm/inc /opt/mqm/inc
COPY --from=dependencies /root/ibm/lib/libcurl.so /opt/mqm/lib/
COPY --from=dependencies /root/ibm/lib/ccsid_part2.tbl /opt/mqm/lib/
COPY --from=dependencies /root/ibm/lib/ccsid.tbl /opt/mqm/lib/
COPY --from=dependencies /root/ibm/lib64/libmqic_r.so /opt/mqm/lib64/
COPY --from=dependencies /root/ibm/lib64/libmqe_r.so /opt/mqm/lib64/
COPY --from=dependencies /root/ibm/gskit8/lib64 /opt/mqm/gskit8/lib64/
ENV LD_LIBRARY_PATH="/opt/mqm/lib64:\${LD_LIBRARY_PATH}"
RUN grep -q ":${user_gid}:" /etc/group || addgroup -g "${user_gid}" grizzly
RUN grep -q ":${user_uid}:" /etc/passwd || adduser -u "${user_uid}" -G grizzly -D grizzly
RUN apk add --no-cache bash git
USER grizzly
RUN pip3 install --user --no-cache-dir -U pip pip-tools
COPY pip-generate-requirements.bash /
CMD ["/pip-generate-requirements.bash", "generate"]
EOF

        docker run --rm -v "${GRIZZLY_MOUNT_CONTEXT}:/mnt" pip-generate-requirements:latest
    fi

    return 0
}

main "$@"
exit $?
