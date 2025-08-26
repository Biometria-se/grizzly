#!/usr/bin/env python

from __future__ import annotations

import argparse
import sys
from json import dumps as jsondumps
from os import environ
from os.path import pathsep
from pathlib import Path
from tempfile import gettempdir


def main() -> int:
    parser = argparse.ArgumentParser(description='Setup environment for GitHub Actions workflow.')
    parser.add_argument('--add-env', dest='env_vars', action='append', type=str, help='Environment variable to add')
    parser.add_argument('--add-path', dest='paths', action='append', type=str, help='Path to add to PATH variable')

    args = parser.parse_args()

    if args.env_vars is None and args.paths is None:
        if args.env_vars is None:
            workspace = Path(environ['GITHUB_WORKSPACE'])
            virtual_env = Path.joinpath(workspace, '.venv')
            virtual_env_path = Path.joinpath(virtual_env, ('Scripts' if sys.platform == 'win32' else 'bin'))
            tmp_dir = Path(gettempdir())
            grizzly_tmp_logfile = Path.joinpath(tmp_dir, 'grizzly.log')

            args.env_vars = [f'VIRTUAL_ENV={virtual_env!s}', f'GRIZZLY_TMP_DIR={tmp_dir!s}', f'GRIZZLY_TMP_LOGFILE={grizzly_tmp_logfile!s}']

        if args.paths is None:
            args.paths = [f'{virtual_env_path!s}']

    if args.paths is not None:
        with Path(environ['GITHUB_PATH']).open('a') as fd:
            fd.writelines(f'{path}\n' for path in args.paths)

        paths_info = jsondumps(args.paths, indent=2)
        print(f'Added paths to PATH variable:\n{paths_info}')

    if args.env_vars is not None:
        with Path(environ['GITHUB_ENV']).open('a') as fd:
            for env_var in args.env_vars:
                key, value = env_var.split('=', 1)
                if key in ['LD_LIBRARY_PATH']:
                    current_value = environ.get(key, '')
                    if current_value:
                        value = f'{value}{pathsep}{current_value}'

                fd.write(f'{key}={value}\n')

        env_var_info = jsondumps(args.env_vars, indent=2)
        print(f'Added environment variables:\n{env_var_info}')

    return 0


if __name__ == '__main__':
    raise SystemExit(main())
