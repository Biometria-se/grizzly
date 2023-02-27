#!/usr/bin/env python3

import sys
import subprocess
import argparse

from os import path, getcwd
from packaging.version import Version


def _parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--from-directory',
        type=str,
        default=getcwd(),
        required=False,
    )

    return parser.parse_args()


def main() -> int:
    args = _parse_arguments()

    git_toplevel_dir = subprocess.check_output(
        ['git', 'rev-parse', '--show-toplevel'],
        cwd=args.from_directory,
    ).decode('utf-8').strip()

    github_project_name = path.basename(git_toplevel_dir)

    output = subprocess.check_output(
        ['git', 'tag'],
        cwd=args.from_directory,
    ).decode('utf-8').strip()

    tags = [v[1:] for v in output.split('\n') if v.startswith('v')]
    if len(tags) > 0:
        tags.sort(reverse=True, key=Version)

    for index, previous_tag in enumerate(tags[1:], start=1):
        previous_tag = f'v{previous_tag}'
        current_tag = f'v{tags[index - 1]}'
        print(f'{github_project_name}: generating changelog for {current_tag} <- {previous_tag}', file=sys.stderr)

        output = subprocess.check_output([
            'git',
            'log',
            f"{previous_tag}...{current_tag}",
            '--oneline',
            '--no-abbrev',
            '--no-merges',
        ], cwd=args.from_directory).decode('utf-8').strip()

        print(f'## {current_tag}\n\n')

        for line in output.split('\n'):
            commit = line[:40]
            commit_short = commit[:8]
            message = line[41:].strip()

            print(f'* <a href="https://github.com/Biometria-se/{github_project_name}/commit/{commit}" target="_blank">`{commit_short}`</a>: {message}\n\n')

    return 0


if __name__ == '__main__':
    sys.exit(main())
