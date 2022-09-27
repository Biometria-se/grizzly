#!/usr/bin/env python3
import sys
import argparse
import os
import subprocess
import re

from tempfile import NamedTemporaryFile
from packaging import version as versioning
from typing import Optional, Dict, Tuple, List, cast
from pathlib import Path

import tomli

from piptools.scripts.compile import cli as pip_compile
from piptools.locations import CACHE_DIR
from click import Context as ClickContext, Command as ClickCommand
from click.globals import push_context as click_push_context

IMAGE_NAME = 'grizzly-pip-compile'

PYPROJECT_PATH = Path(__file__).parent / '..' / 'pyproject.toml'


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
    with open(PYPROJECT_PATH, 'rb') as fd:
        config = tomli.load(fd)

    if 'project' not in config:
        raise AttributeError('pyproject.toml is missing [project] section')

    python_requires: Optional[str] = config['project'].get('requires-python', None)

    if python_requires is None:
        raise AttributeError('could not find requires-python in pyproject.toml')

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


def build_container_image(python_version: str) -> Tuple[int, Dict[str, str]]:
    print(f'running pip-compile with python {python_version}', flush=True)

    build_context = Path(__file__).parent

    with NamedTemporaryFile(prefix='Dockerfile.', delete=True, mode='w', dir=build_context) as fd:
        user_gid = getgid()
        user_uid = getuid()
        fd.write(f'''# this is need because pip-compile needs them (!!) when compiling requirements*.txt file, with pymqi included
FROM alpine:latest as dependencies
USER root
RUN mkdir /root/ibm && cd /root/ibm && \
    wget https://public.dhe.ibm.com/ibmdl/export/pub/software/websphere/messaging/mqdev/redist/9.2.5.0-IBM-MQC-Redist-LinuxX64.tar.gz -O - | tar xzf -

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

        command = ['docker', 'image', 'build', '-t', f'{IMAGE_NAME}:{python_version}', str(build_context), '-q', '-f', fd.name]

        return run_command(command)


def run_container(python_version: str) -> Tuple[int, Dict[str, str]]:
    command = [
        'docker',
        'container',
        'run',
        '-v', f"{os.getenv('GRIZZLY_MOUNT_CONTEXT')}:/mnt",
        '--name', IMAGE_NAME,
        f'{IMAGE_NAME}:{python_version}',
    ]

    return run_command(command)


def run_command(command: List[str]) -> Tuple[int, Dict[str, str]]:
    output: Dict[str, str] = []
    process = subprocess.Popen(
        command,
        shell=False,
        close_fds=True,
        stderr=subprocess.STDOUT,
        stdout=subprocess.PIPE,
    )

    try:
        while process.poll() is None:
            stdout = process.stdout

            if stdout is None:
                break

            buffer = stdout.readline()
            if not buffer:
                break

            output.append(buffer.decode('utf-8'))

        process.terminate()
    except KeyboardInterrupt:
        pass
    finally:
        try:
            process.kill()
        except Exception:
            pass

    process.wait()

    return process.returncode, output


def has_git_dependencies(root_path: Path) -> bool:
    if not root_path.is_file():
        pyproject_file = root_path / 'pyproject.toml'
    else:
        pyproject_file = root_path

    with open(pyproject_file, 'rb') as fd:
        config = tomli.load(fd)

    if 'project' in config:
        if any([True if 'git+' in dependency else False for dependency in config['project']['dependencies']]):
            return True

    if 'optional-dependencies' in config['project']:
        options_extras = config['project']['optional-dependencies']

        for dependencies in options_extras.values():
            if any([True if 'git+' in dependency else False for dependency in dependencies]):
                return True

    return False


def compile(target: Optional[str] = None) -> int:
    if Path('/mnt/pyproject.toml').exists():
        root_path = Path('/mnt')
    else:
        root_path = Path(__file__).parent / '..'

    generate_hashes = not has_git_dependencies(root_path)

    targets_all: Dict[str, Tuple[str, ...]] = {
        'requirements.txt': (),
        'requirements-ci.txt': ('dev', 'ci', ),
        'requirements-dev.txt': ('dev', 'mq', 'ci', ),
        'requirements-docs.txt': (),
    }

    if target is None:
        targets = targets_all
    else:
        targets = {target: targets_all[target]}

    click_push_context(ClickContext(command=ClickCommand(name='cli')))

    os.environ['CUSTOM_COMPILE_COMMAND'] = 'script/pip-compile.py'

    rc = 0

    for target, extras in targets.items():
        output_file = root_path / target

        base, _ = os.path.splitext(output_file)
        if os.path.exists(f'{base}.in'):
            src_files = (f'{base}.in', )
            generate_hashes = not has_git_dependencies(Path(src_files[0]))
        else:
            src_files = ('pyproject.toml', )

        with open(output_file, 'w+b') as fd:
            print(f'generating {target} from {src_files[0]}', flush=True)
            try:
                pip_compile.callback(
                    verbose=1,
                    quiet=0,  # <!-- --quiet
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
                    resolver_name='cli',
                )
            except SystemExit as e:
                rc += e.code
            finally:
                continue

    return rc


def main() -> int:
    args = parse_arguments()

    if args.compile:
        rc = compile(args.target)
    else:
        python_version = args.python_version
        if python_version is None:
            python_version = get_python_version()

        rc, output = build_container_image(python_version)

        if rc != 0:
            print(''.join(output))

        if rc == 0:
            rc, output = run_container(python_version)

            if rc != 0 and len(output) > 0:
                print(''.join(output))
                rc, output = run_command([
                    'docker', 'container', 'logs', 'grizzly-pip-compile',
                ])

                print(''.join(output))

            rc, output = run_command([
                'docker', 'container', 'rm', 'grizzly-pip-compile',
            ])

    return rc


if __name__ == '__main__':
    sys.exit(main())
