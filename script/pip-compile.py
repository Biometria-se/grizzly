#!/usr/bin/env python3
import sys
import argparse
import os
import subprocess
import re

from tempfile import NamedTemporaryFile
from packaging import version as versioning
from typing import Optional, Dict, Tuple, cast
from configparser import ConfigParser

from piptools.scripts.compile import cli as pip_compile
from piptools.locations import CACHE_DIR
from click import Context as ClickContext, Command as ClickCommand
from click.globals import push_context as click_push_context

IMAGE_NAME = 'grizzly-pip-compile'


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description='use pip-compile to compile requirements files for grizzly, should be used after updating dependencies.',
    )

    parser.add_argument(
        '--target',
        type=str,
        required=False,
        default=None,
        help='only generate the specified targets, instead of all',
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
        help='specify python version instead of finding a suitable in setup.cfg',
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
    config = ConfigParser()
    setup_path = os.path.realpath(os.path.join(os.path.dirname(__file__), '..', 'setup.cfg'))
    config.read(setup_path)

    if 'options' not in config:
        raise AttributeError('setup.cfg is missing [options] section')

    python_requires: Optional[str] = config['options'].get('python_requires', None)

    if python_requires is None:
        raise AttributeError('could not find python_requires in setup.cfg')

    version = re.sub(r'[^0-9\.]', '', python_requires)

    parsed_version = versioning.parse(version)
    version_minor = parsed_version.minor

    if '>' in python_requires:
        compare, step = (parsed_version.__le__ if '=' in python_requires else parsed_version.__lt__, 1, )
    elif '<' in python_requires:
        compare, step = (parsed_version.__ge__ if '=' in python_requires else parsed_version.__gt__, -1, )
    else:
        compare, step = (parsed_version.__eq__, 0, )

    while not compare(versioning.parse(f'{parsed_version.major}.{version_minor}')):
        if (version_minor < 0 or version_minor > parsed_version.minor * 2):
            raise RuntimeError(f'unable to find a suitable version based on {python_requires}')

        version_minor += step

    return f'{parsed_version.major}.{version_minor}'


def build_container_image(python_version: str) -> int:
    print(f'running pip-compile with python {python_version}', flush=True)

    build_context = os.path.dirname(__file__)

    with NamedTemporaryFile(delete=True, mode='w', dir=build_context) as fd:
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
RUN pip3 install --user --no-cache-dir -U pip pip-tools packaging
WORKDIR /mnt
COPY pip-compile.py /
ENV PYTHONUNBUFFERED=1
CMD ["/pip-compile.py", "--compile"]
''')

        fd.flush()

        rc = subprocess.check_call([
            'docker', 'image', 'build', '-t', f'{IMAGE_NAME}:{python_version}', build_context, '-q', '-f', fd.name,
        ], shell=False, close_fds=True)

    return rc


def run_container(python_version: str) -> int:
    return subprocess.check_call([
        'docker',
        'container',
        'run',
        '--rm',
        '-v', f"{os.getenv('GRIZZLY_MOUNT_CONTEXT')}:/mnt",
        '--name', IMAGE_NAME,
        f'{IMAGE_NAME}:{python_version}',
    ], shell=False, close_fds=True)


def has_git_dependencies(where_am_i: str) -> bool:
    setup_file = os.path.join(where_am_i, 'setup.cfg')
    config = ConfigParser()
    config.read(setup_file)

    if 'options' in config:
        if 'git+' in config['options'].get('install_requires'):
            return True

    if 'options.extras_require' in config:
        options_extras_require = config['options.extras_require']

        for option in options_extras_require:
            if 'git+' in options_extras_require[option]:
                return True

    return False


def compile(target: Optional[str] = None) -> int:
    if os.path.exists('/mnt/setup.cfg'):
        where_am_i = '/mnt'
    else:
        where_am_i = os.path.realpath(os.path.join(os.path.dirname(__file__), '..'))

    generate_hashes = not has_git_dependencies(where_am_i)

    targets_all: Dict[str, Tuple[str, ...]] = {
        'requirements.txt': (),
        'requirements-ci.txt': ('dev', 'ci', 'script', ),
        'requirements-dev.txt': ('dev', 'mq', 'script', ),
        'requirements-script.txt': (),
    }

    if target is None:
        targets = targets_all
    else:
        targets = {target: targets_all[target]}

    click_push_context(ClickContext(command=ClickCommand(name='cli')))

    os.environ['CUSTOM_COMPILE_COMMAND'] = 'script/pip-compile.py'

    for target, extras in targets.items():
        output_file = os.path.join(where_am_i, target)
        base, _ = os.path.splitext(output_file)
        if os.path.exists(f'{base}.in'):
            src_files = (f'{base}.in', )
        else:
            src_files = ('pyproject.toml', )

        with open(os.path.join(where_am_i, target), 'w+b') as fd:
            print(f'generating {target} from {src_files[0]}', flush=True)
            try:
                pip_compile.callback(
                    verbose=0,
                    quiet=1,  # <!-- --quiet
                    dry_run=False,
                    pre=None,
                    rebuild=False,
                    extras=extras,  # <!-- --extras <section>, ...
                    find_links=(),
                    index_url=None,
                    extra_index_url=(),
                    cert=None,
                    client_cert=None,
                    trusted_host=(),
                    header=True,
                    emit_trusted_host=True,
                    annotate=True,
                    annotation_style='split',
                    upgrade=False,
                    upgrade_packages=(),
                    output_file=fd,  # <!-- -o
                    allow_unsafe=True,
                    strip_extras=False,
                    generate_hashes=generate_hashes,  # <!-- --generate-hashes
                    reuse_hashes=True,
                    src_files=src_files,
                    max_rounds=10,
                    build_isolation=True,
                    emit_find_links=True,
                    cache_dir=CACHE_DIR,
                    pip_args_str=' '.join([  # <!-- --pip-args "..."
                        '--disable-pip-version-check',
                        '--no-cache-dir',
                        '--user',
                        '--no-warn-script-location',
                    ]),
                    emit_index_url=True,
                    emit_options=True,
                )
            except SystemExit as e:
                return e.code

    return 0


def main() -> int:
    args = parse_arguments()

    if args.compile:
        rc = compile(args.target)
    else:
        python_version = args.python_version
        if python_version is None:
            python_version = get_python_version()

        rc = build_container_image(python_version)

        if rc == 0:
            rc = run_container(python_version)

    return rc


if __name__ == '__main__':
    sys.exit(main())
