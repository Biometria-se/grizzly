#!/usr/bin/env python3
import sys
import argparse
import ast
import os
import subprocess
import re

from tempfile import NamedTemporaryFile
from packaging import version as versioning
from typing import Optional, cast


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description='use pip-compile to compile requirements files for grizzly, should be used after updating dependencies.',
    )

    group = parser.add_mutually_exclusive_group()

    group.add_argument(
        '--compile',
        action='store_true',
        default=False,
        help='compile with python version in environment',
    )

    group.add_argument(
        '--python-version',
        type=str,
        default=None,
        required=False,
        help='specify python version instead of finding a suitable in setup.py',
    )

    return parser.parse_args()

def getuid() -> int:
    if os.name == 'nt' or not hasattr(os, 'getuid'):
        return 1000
    else:
        return cast(int, getattr(os, 'getuid')())


def getgid() -> int:
    if os.name == 'nt' or not hasattr(os, 'getgid'):
        return 1000
    else:
        return cast(int, getattr(os, 'getgid')())

def get_python_version() -> str:
    setup_py_path = os.path.realpath(os.path.join(os.path.dirname(__file__), '..', 'setup.py'))
    required_version: Optional[str] = None

    with open(setup_py_path) as fd:
        for node in ast.walk(ast.parse(fd.read())):
            if not isinstance(node, ast.keyword):
                continue

            if not node.arg == 'python_requires':
                continue

            if not isinstance(node.value, ast.Constant):
                raise ValueError('python_requires in setup.py/setup() is not specified as an constant')

            required_version = node.value.value
            break

    if required_version is None:
        raise AttributeError('could not find required_version in setup.py/setup()')

    version = re.sub(r'[^0-9\.]', '', required_version)

    parsed_version = versioning.parse(version)
    version_minor = parsed_version.minor

    if '>' in required_version:
        compare, step = (parsed_version.__le__ if '=' in required_version else parsed_version.__lt__, 1, )
    elif '<' in required_version:
        compare, step = (parsed_version.__ge__ if '=' in required_version else parsed_version.__gt__, -1, )
    else:
        compare, step = (parsed_version.__eq__, 0, )

    while not compare(versioning.parse(f'{parsed_version.major}.{version_minor}')):
        if (version_minor < 0 or version_minor > parsed_version.minor * 2):
            raise RuntimeError(f'unable to find a suitable version based on {required_version}')

        version_minor += step

    return f'{parsed_version.major}.{version_minor}'


def build_container_image(python_version: str) -> int:
    print(f'running pip-compile with python {python_version}')

    with NamedTemporaryFile(delete=False, mode='w') as fd:
        user_gid = getgid()
        user_uid = getuid()
        fd.write(f'''# this is need because pip-compile needs them (!!) when compiling requirements*.txt file, with pymqi included
FROM alpine:latest as dependencies
USER root
RUN mkdir /root/ibm && cd /root/ibm && \
    wget https://public.dhe.ibm.com/ibmdl/export/pub/software/websphere/messaging/mqdev/redist/9.2.2.0-IBM-MQC-Redist-LinuxX64.tar.gz -O - | tar xzf -

FROM python:{python_version}-alpine
RUN mkdir -p /opt/mqm/lib64 && mkdir /opt/mqm/lib && mkdir -p /opt/mqm/gskit8/lib64
COPY --from=dependencies /root/ibm/inc /opt/mqm/inc
COPY --from=dependencies /root/ibm/lib/libcurl.so /opt/mqm/lib/
COPY --from=dependencies /root/ibm/lib/ccsid_part2.tbl /opt/mqm/lib/
COPY --from=dependencies /root/ibm/lib/ccsid.tbl /opt/mqm/lib/
COPY --from=dependencies /root/ibm/lib64/libmqic_r.so /opt/mqm/lib64/
COPY --from=dependencies /root/ibm/lib64/libmqe_r.so /opt/mqm/lib64/
COPY --from=dependencies /root/ibm/gskit8/lib64 /opt/mqm/gskit8/lib64/
ENV LD_LIBRARY_PATH="/opt/mqm/lib64:\\${{LD_LIBRARY_PATH}}"
RUN grep -q ":{user_gid}:" /etc/group || addgroup -g "{user_gid}" grizzly
RUN grep -q ":{user_uid}:" /etc/passwd || adduser -u "{user_uid}" -G grizzly -D grizzly
RUN apk add --no-cache bash git
USER grizzly
RUN pip3 install --user --no-cache-dir -U pip pip-tools
COPY pip-compile.py /
CMD ["/pip-compile.py", "--compile"]
''')

        fd.flush()

        print(fd.name)
        subprocess.check_call([
            'docker', 'image', 'build', '-t', 'grizzly-pip-compile:latest', os.path.dirname(__file__), '-q', '-f', fd.name,
        ], shell=False, close_fds=True)

    return 0


def main() -> int:
    args = parse_arguments()

    if args.compile:
        pass
    else:
        python_version = args.python_version
        if python_version is None:
            python_version = get_python_version()

        build_container_image(python_version)

    return 0


if __name__ == '__main__':
    sys.exit(main())
